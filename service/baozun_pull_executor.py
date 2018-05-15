from datetime import datetime, timedelta
from math import ceil, floor

from concurrent import futures
from zato.server.service import Service

from com.oocl.gt.baozun.baozunpull import *
from com.oocl.gt.util.servicelock import ServiceLock

dt_format = '%Y-%m-%d %H:%M:%S'
dmtp_service = 'dmtpservice.dmtp-service'

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
    'pageSize': 10,
    'thread_count': 3,
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
        if total > 0:
            if total >= config['pageSize'] * config['thread_count']:
                batchPull = BaozunBatchPull(baozunWS=baozunWS, order_type='ASN')
                ttlPages = int(ceil(total / config['pageSize']))
                s = int(floor(ttlPages / config['thread_count']))
                rg = range(1, ttlPages, s)
                rg[-1] = ttlPages + 1
                with futures.ProcessPoolExecutor(max_workers=config['thread_count']) as exe:
                    i = 1
                    fs = []
                    while i < len(rg):
                        f = exe.submit(fn=self._run, batchPull=batchPull, startTime=st, endTime=et,
                                       pages=range(rg[i - 1], rg[i]),
                                       pageSize=config['pageSize'], order_type=order_type)
                        fs.append(f)
                        i += 1
                    for f in fs:
                        f.result()
            else:
                req, rep = pull.run({'startTime': st, 'endTime': et, 'page': 1, 'pageSize': total})
                msg = json.loads(rep)['entry']
                self._call_dmtp(uri=config['dmtpUrl'][order_type], data=msg)
            self.service.kvdb.conn.set(config['last_time'][order_type], et)

    def _call_dmtp(self, uri, data):
        return self.service.async_invoke(dmtp_service, {'url': config['dmtpServer'] + uri, 'data': data})

    def _run(self, batchPull, startTime, endTime, pages, pageSize, orderType):
        (msgs, failed, total) = batchPull.run(
            {'startTime': startTime, 'endTime': endTime, 'pages': pages, 'pageSize': pageSize})
        if len(failed) > 0:
            raise Exception('failed orders: %s', json.dumps(failed))
        for msg in msgs:
            self._call_dmtp(uri=config['dmtpUrl'][orderType], data=msg)


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
