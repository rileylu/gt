from datetime import datetime, timedelta
from math import ceil, floor

from zato.server.service import Service
import threading
import traceback
import sys

from com.oocl.gt.baozun.baozunws import *
from com.oocl.gt.baozun.baozunpull import *
from com.oocl.gt.util.servicelock import ServiceLock

dt_format = '%Y-%m-%d %H:%M:%S'
dmtp_service = 'dmtpservice.dmtp-service'
logfile_service = 'logfile.log-file'

config = {
    'url': 'https://hub-test.baozun.cn/web-service/warehouse/1.0?wsdl',
    "dmtpServer": "http://wmsuat.oocllogistics.com",
    "dmtpUrl": {
        "ITEM": "/DMTP_AEServer/HttpService?profileName=GUAW0058_20024904CRR_NIKE_Inbound_Item",
        "ASN": "/DMTP_AEServer/HttpService?profileName=GUAW0058_20024904CRR_NIKE_Inbound_ASN",
        "SPO": "/DMTP_AEServer/HttpService?profileName=GUAW0058_20024904CRR_SPO_AND_SALES",
        "SPO_SALES": "/DMTP_AEServer/HttpService?profileName=GUAW0058_20024904CRR_SPO_AND_SALES",
    },
    'customer': 'WH_OCL',
    'key': 'abcdef',
    'sign': '123456',
    'pageSize': 50,
    'thread_count': 4,
    'last_time':
        {
            'ASN': 'ASN_LAST_TIME',
            'SPO': 'SPO_LAST_TIME',
            'ITEM': 'ITEM_LAST_TIME',
            'SPO_SALES': 'SPO_SALES_LAST_TIME'
        }
}


class BaozunCronExecutor:
    class BatchRunThread(threading.Thread):
        def __init__(self, pull, startTime, endTime, pages, pageSize, orderType, service):
            threading.Thread.__init__(self)
            self.pull = pull
            self.startTime = startTime
            self.endTime = endTime
            self.pages = pages
            self.pageSize = pageSize
            self.orderType = orderType
            self.service = service
            self.exitcode = 0
            self.exception = None
            self.exc_traceback = ''

        def run(self):
            try:
                for p in self.pages:
                    req, rep = self.pull.run(
                        {'startTime': self.startTime, 'endTime': self.endTime, 'page': p, 'pageSize': self.pageSize})
                    self._call_dmtp(uri=config['dmtpUrl'][self.orderType], data=rep)
                    self._write_to_file('%s_%s_%s_%d' % (self.orderType, self.startTime, self.endTime, p), rep)
            except Exception, e:
                self.exitcode = 1
                self.exception = e
                self.exc_traceback = ''.join(traceback.format_exception(*sys.exc_info()))

        def _call_dmtp(self, uri, data):
            data = json.dumps(json.loads(data))
            self.service.invoke(dmtp_service, {'url': '%s%s' % (config['dmtpServer'], uri), 'data': data})

        def _write_to_file(self, fn, data):
            data = json.dumps(json.loads(data))
            self.service.invoke_async(logfile_service, {'path': fn, 'data': data, 'type': 'json'})

    def __init__(self, service):
        self.service = service

    def run(self, order_type):
        if order_type not in config['last_time']:
            raise Exception('ORDER TYPE %s IS NOT DEFINED' % order_type)
        st = self.service.kvdb.conn.get(config['last_time'][order_type])
        if st is None:
            st = (datetime.now() - timedelta(days=1)).strftime(dt_format)
        et = datetime.now().strftime(dt_format)
        baozunWS = BaozunWebService(url=config['url'], cus=config['customer'], key=config['key'],
                                    sign=config['sign'])
        pull = BaozunPullOnce(baozunWS=baozunWS, order_type=order_type)
        req, rep = pull.run(
            {'startTime': st, 'endTime': et, 'page': 1, 'pageSize': 1})
        rep_json = json.loads(rep)
        total = rep_json['total']
        self.service.logger.info('<startTime:%s , endTime:%s , total:%d>' % (st, et, total))
        success = True
        if total > 0:
            if total >= config['pageSize'] * config['thread_count']:
                self.service.logger.info('start multiple thread')
                ttlPages = int(ceil(total / config['pageSize']))
                s = int(floor(ttlPages / config['thread_count']))
                rg = range(1, ttlPages, s)
                rg[-1] = ttlPages + 1
                i = 1
                tds = []
                while i < len(rg):
                    td = BaozunCronExecutor.BatchRunThread(pull, st, et, range(rg[i - 1], rg[i]), config['pageSize'],
                                                           order_type, self.service)
                    td.start()
                    tds.append(td)
                    i += 1
                for td in tds:
                    td.join()
                    if td.exitcode != 0:
                        self.service.logger.warn(td.exception.message)
                        success = False
            else:
                td = BaozunCronExecutor.BatchRunThread(pull, st, et, [1, ], total, order_type, self.service)
                td.run()
                if td.exitcode != 0:
                    self.service.logger.warn(td.exception.message)
                    success = False
            if success:
                self.service.kvdb.conn.set(config['last_time'][order_type], et)


class BaozunPullASN(Service):
    def handle(self):
        try:
            with ServiceLock(self):
                executor = BaozunCronExecutor(service=self)
                executor.run('ASN')
        except Exception, e:
            self.logger.error(e.message)


class BaozunPullSPO(Service):

    def handle(self):
        try:
            with ServiceLock(self):
                executor = BaozunCronExecutor(service=self)
                executor.run('SPO')
        except Exception, e:
            self.logger.error(e.message)


class BaozunPullITEM(Service):

    def handle(self):
        try:
            with ServiceLock(self):
                executor = BaozunCronExecutor(service=self)
                executor.run('ITEM')
        except Exception, e:
            self.logger.error(e.message)


class BaozunPullSPO_SALES(Service):

    def handle(self):
        try:
            with ServiceLock(self):
                executor = BaozunCronExecutor(service=self)
                executor.run('SPO_SALES')
        except Exception, e:
            self.logger.error(e.message)


class RangeBasedPull(Service):
    def handle(self):
        try:
            with ServiceLock(self):
                pass
        except Exception, e:
            self.logger.error(e.message)
