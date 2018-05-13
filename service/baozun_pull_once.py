from zato.server.service import Service
from com.oocl.gt.baozun.baozunws import BaozunWebService
import logging
import json
import time

config = {
    'url': 'https://hub-test.baozun.cn/web-service/warehouse/1.0?wsdl',
    'customer': 'WH_OCL',
    'key': 'abcdef',
    'sign': '123456'
}


class BaozunException(Exception):
    pass


class BaozunPullOnce(Service):
    class SimpleIO:
        input_required = ('type', 'startTime', 'endTime', 'pageSize', 'page')
        output_required = ('total', 'message')

    def __init__(self):
        Service.__init__(self)
        self.baozun_ws = BaozunWebService(url=config['url'], cus=config['customer'], key=config['key'],
                                          sign=config['sign'])
        self._rep_req_logger = logging.getLogger('repreq')

    def handle(self):
        service = {
            'ASN': self.baozun_ws.pull_asn,
            'SPO': self.baozun_ws.pull_spo,
            'SALES_SPO': self.baozun_ws.pull_sales_order,
            'ITEM': self.baozun_ws.pull_sku
        }
        t = self.request.input.type
        if t not in service:
            raise BaozunException('%s NOT DEFINED' % t)
        service = service[t]
        startTime = self.request.input.startTime
        endTime = self.request.input.endTime
        page = int(self.request.input.page)
        pageSize = int(self.request.input.pageSize)
        self.response.payload.total = 0
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
                        time.sleep(3)
                    else:
                        raise BaozunException(
                            'CALLING BAOZUN SERVICE WITH ERROR: (%s,%s)' % (msg['errorCode'], msg['msg']))
                else:
                    self.response.payload.total = rep['total']
                    self.response.payload.msg = rep['message']
                    return
        except Exception as e:
            raise BaozunException('CALLING BAOZUN SERVICE WITH ERROR: %s' % e.message)
