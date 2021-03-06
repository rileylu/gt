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
fn_format = '%Y%m%d%H%M%S'
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
                    param = {'startTime': self.startTime, 'endTime': self.endTime, 'page': p, 'pageSize': self.pageSize}
                    self.service.logger.info(json.dumps(param))
                    req, rep = self.pull.run(param)
                    self._call_dmtp(uri=config['dmtpUrl'][self.orderType], data=rep)
                    self._write_to_file(self.startTime, self.endTime, p, self.orderType, rep)
            except Exception, e:
                self.exitcode = 1
                self.exception = e
                self.exc_traceback = ''.join(traceback.format_exception(*sys.exc_info()))

        def _call_dmtp(self, uri, data):
            data = json.dumps(json.loads(data))
            self.service.invoke(dmtp_service, {'url': '%s%s' % (config['dmtpServer'], uri), 'data': data})

        def _write_to_file(self, startTime, endTime, page, orderType, data):
            st = datetime.strptime(startTime, dt_format).strftime(fn_format)
            et = datetime.strptime(endTime, dt_format).strftime(fn_format)
            data = json.dumps(json.loads(data))
            self.service.invoke_async(logfile_service,
                                      {'path': '%s_%s_%s_%d' % (orderType, st, et, page), 'data': data, 'type': 'json'})

    def __init__(self, service):
        self.service = service

    def run(self, startTime, endTime, pageSize, thread_count, order_type):
        if order_type not in config['last_time']:
            raise Exception('ORDER TYPE %s IS NOT DEFINED' % order_type)
        st = startTime
        et = endTime
        baozunWS = BaozunWebService(url=config['url'], cus=config['customer'], key=config['key'],
                                    sign=config['sign'])
        pull = BaozunPullOnce(baozunWS=baozunWS, order_type=order_type)
        req, rep = pull.run(
            {'startTime': st, 'endTime': et, 'page': 1, 'pageSize': 1})
        rep_json = json.loads(rep)
        total = rep_json['total']
        self.service.logger.info('<startTime:%s , endTime:%s , total:%d>' % (st, et, total))
        if total > 0:
            success = True
            if total >= pageSize * thread_count:
                self.service.logger.info('start multiple thread')
                ttlPages = int(ceil(float(total) / pageSize))
                s = int(floor(float(ttlPages) / thread_count))
                rg = range(1, ttlPages, s)
                rg.append(ttlPages + 1)
                i = 1
                tds = []
                while i < len(rg):
                    td = BaozunCronExecutor.BatchRunThread(pull, st, et, range(rg[i - 1], rg[i]), pageSize,
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
                return success
        return False


class BaozunPullASN(Service):
    def handle(self):
        try:
            with ServiceLock(self):
                st = self.kvdb.conn.get(config['last_time']['ASN'])
                if st is None:
                    st = (datetime.now() - timedelta(days=1)).strftime(dt_format)
                et = datetime.now().strftime(dt_format)
                executor = BaozunCronExecutor(service=self)
                res = executor.run(st, et, config['pageSize'], config['thread_count'], 'ASN')
                if res:
                    self.kvdb.conn.set(config['last_time']['ASN'], et)
        except Exception, e:
            self.logger.error(e.message)


class BaozunPullSPO(Service):

    def handle(self):
        try:
            with ServiceLock(self):
                st = self.kvdb.conn.get(config['last_time']['SPO'])
                if st is None:
                    st = (datetime.now() - timedelta(days=1)).strftime(dt_format)
                et = datetime.now().strftime(dt_format)
                executor = BaozunCronExecutor(service=self)
                res = executor.run(st, et, config['pageSize'], config['thread_count'], 'SPO')
                if res:
                    self.kvdb.conn.set(config['last_time']['SPO'], et)
        except Exception, e:
            self.logger.error(e.message)


class BaozunPullITEM(Service):

    def handle(self):
        try:
            with ServiceLock(self):
                st = self.kvdb.conn.get(config['last_time']['ITEM'])
                if st is None:
                    st = (datetime.now() - timedelta(days=1)).strftime(dt_format)
                et = datetime.now().strftime(dt_format)
                executor = BaozunCronExecutor(service=self)
                res = executor.run(st, et, config['pageSize'], config['thread_count'], 'ITEM')
                if res:
                    self.kvdb.conn.set(config['last_time']['ITEM'], et)
        except Exception, e:
            self.logger.error(e.message)


class BaozunPullSPO_SALES(Service):

    def handle(self):
        try:
            with ServiceLock(self):
                st = self.kvdb.conn.get(config['last_time']['SPO_SALES'])
                if st is None:
                    st = (datetime.now() - timedelta(days=1)).strftime(dt_format)
                et = datetime.now().strftime(dt_format)
                executor = BaozunCronExecutor(service=self)
                res = executor.run(st, et, config['pageSize'], config['thread_count'], 'SPO_SALES')
                if res:
                    self.kvdb.conn.set(config['last_time']['SPO_SALES'], et)
        except Exception, e:
            self.logger.error(e.message)


class RangeBasedPull(Service):

    def handle(self):
        try:
            with ServiceLock(self):
                req = json.loads(self.request.raw_request)
                executor = BaozunCronExecutor(service=self)
                executor.run(
                    req['starttime'],
                    req['endtime'],
                    int(req['pagesize']),
                    int(req['threadcount']),
                    req['ordertype']
                )
        except Exception, e:
            self.logger.error(e.message)
