from datetime import datetime, timedelta
from math import ceil, floor

from zato.server.service import Service
import threading

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
    'pageSize': 2,
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
        if total > 0:
            if total >= config['pageSize'] * config['thread_count']:
                self.service.logger.info('start multiple thread')
                batchPull = BaozunBatchPull(baozunWS=baozunWS, order_type=order_type)
                ttlPages = int(ceil(total / config['pageSize']))
                s = int(floor(ttlPages / config['thread_count']))
                rg = range(1, ttlPages, s)
                rg[-1] = ttlPages + 1
                tds = []
                i = 1
                while i < len(rg):
                    td = threading.Thread(target=self._run, args=(
                        batchPull, st, et, range(rg[i - 1], rg[i]), config['pageSize'], order_type))
                    td.start()
                    tds.append(td)
                    i += 1
                for td in tds:
                    td.join()
            else:
                req, rep = pull.run({'startTime': st, 'endTime': et, 'page': 1, 'pageSize': total})
                msg = json.loads(rep)['entry']
                self._call_dmtp(uri=config['dmtpUrl'][order_type], data=msg)
                self._write_to_file('%s_%s' % (st, et), msg)
            self.service.kvdb.conn.set(config['last_time'][order_type], et)

    def _call_dmtp(self, uri, data):
        self.service.invoke_async(dmtp_service, {'url': '%s%s' % (config['dmtpServer'], uri), 'data': data})

    def _write_to_file(self, fn, data):
        self.service.invoke_async(logfile_service, {'path': fn, 'data': data, 'type': 'json'})

    def _run(self, batchPull, startTime, endTime, pages, pageSize, order_type):
        self.service.logger.info('<startTime:%s,endTime:%s,pages:%s,pagesize:%s,ordertype:%s>' % (
            startTime, endTime, json.dumps(pages), pageSize, order_type))
        (msgs, failed) = batchPull.run(
            {'startTime': startTime, 'endTime': endTime, 'pages': pages, 'pageSize': pageSize})
        for msg in msgs:
            self._call_dmtp(uri=config['dmtpUrl'][order_type], data=msg)
            self._write_to_file('%s_%s' % (startTime, endTime), msg)
        if len(failed) > 0:
            raise Exception('failed orders: %s', json.dumps(failed))


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
