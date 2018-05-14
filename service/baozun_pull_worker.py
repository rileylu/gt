from com.oocl.gt.baozun.baozunws import BaozunWebService
from zato.server.service import Service
import json
import logging
import time

config = {
    'url': 'https://hub-test.baozun.cn/web-service/warehouse/1.0?wsdl',
    'customer': 'WH_OCL',
    'key': 'abcdef',
    'sign': '123456',
    "dmtpServer": "http://wmsuat.oocllogistics.com",
    "dmtpUrl":
        {
            "ITEM": "/DMTP_AEServer/HttpService?profileName=GUAW0058_20024904CRR_NIKE_Inbound_Item",
            "ASN": "/DMTP_AEServer/HttpService?profileName=GUAW0058_20024904CRR_NIKE_Inbound_ASN",
            "SPO": "/DMTP_AEServer/HttpService?profileName=GUAW0058_20024904CRR_SPO_AND_SALES",
            "SPO_SALES": "/DMTP_AEServer/HttpService?profileName=GUAW0058_20024904CRR_SPO_AND_SALES",
        },
}


class DMTPException(Exception):
    pass


class BaozunException(Exception):
    pass


class BaozunPullOnce:
    def __init__(self, order_type):
        self.ws = BaozunWebService(url=config['url'], cus=config['customer'], key=config['key'], sign=config['sign'])
        self.order_type = order_type
        self._rep_req_logger = logging.getLogger('repreq')

    def run(self, startTime, endTime, page, pageSize):
        service = {
            'ASN': self.ws.pull_asn,
            'SPO': self.ws.pull_spo,
            'SALES_SPO': self.ws.pull_sales_order,
            'ITEM': self.ws.pull_sku
        }
        if self.order_type not in service:
            raise BaozunException('OrderType %s NOT DEFINED' % self.order_type)
        service = service[self.order_type]
        try:
            retry_count = 3
            while retry_count > 0:
                (req, rep) = service(startTime=startTime, endTime=endTime, page=page, pageSize=pageSize)
                self._rep_req_logger.info(req)
                self._rep_req_logger.info(rep)
                rep = json.loads(rep)
                msg = json.loads(rep['message'])
                if 'errorCode' in msg:
                    if retry_count > 0:
                        retry_count -= 1
                        time.sleep(1)
                    else:
                        raise BaozunException(
                            'CALLING BAOZUN SERVICE WITH ERROR: (%s,%s)' % (msg['errorCode'], msg['msg']))
                else:
                    return rep['message']
        except Exception as e:
            raise BaozunException('CALLING BAOZUN SERVICE WITH ERROR: %s' % e.message)


class BaozunPullWorkerService(Service):
    class SimpleIO:
        input_required = ('startTime', 'endTime', 'pages', 'pageSize', 'type')
        output_required = ('failed',)

    def handle(self):
        startTime = self.request.input.startTime
        endTime = self.request.input.endTime
        pages = json.loads(self.request.input.pages)
        pageSize = int(self.request.input.pageSize)
        service = self.request.input.type
        run_once = BaozunPullOnce(service)
        self.response.payload.failed = []
        for p in pages:
            p = int(p)
            try:
                rep = run_once.run(startTime=startTime, endTime=endTime, page=p, pageSize=pageSize)
                self._dmtpCall(config['dmtpServer'] + config['dmtpUrl'][self.request.input.service], rep)
                fn = '%s_%s_%d.json' % (startTime, endTime, p)
                self._write_to_file(fn=fn, data=json.dumps(rep))
            except Exception as e:
                self.logger.error(e)
                self.response.payload.failed.append(p)

    def _write_to_file(self, fn, data):
        self.invoke_async('logfile.log-file', {'path': '%s' % fn, 'data': data, 'type': 'json'})

    def _dmtpCall(self, url, msg):
        try:
            return self.invoke('dmtpservice.dmtp-service', {'url': url, 'data': msg})
        except Exception as e:
            raise DMTPException('CALLING DMTP WITH ERROR: %s' % e.message)
