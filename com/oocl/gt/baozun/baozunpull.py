from com.oocl.gt.baozun.baozunws import BaozunWebService
from abc import abstractmethod, ABCMeta
import logging
import json


class BaozunPullException(Exception):
    pass


class BaozunPull:
    __metaclass__ = ABCMeta

    def __init__(self, url, customer, key, sign, order_type):
        ws = BaozunWebService(url=url, cus=customer, key=key, sign=sign)
        service = {
            "ASN": ws.pull_asn,
            "ITEM": ws.pull_sku,
            "SPO": ws.pull_spo,
            "SPO_SALES": ws.pull_sales_order
        }
        if order_type not in service:
            raise BaozunPullException("ORDER_TYPE %s IS NOT DEFINED")
        self.service = service[order_type]
        self.logger = logging.getLogger('baozun_pull')

    @abstractmethod
    def run(self, param):
        pass


class BaozunPullOnce(BaozunPull):
    def run(self, param):
        (req, rep) = self.service(startTime=param['startTime'], endTime=param['endTime'], page=param['page'],
                                  pageSize=param['pageSize'])
        self.logger.info(req)
        self.logger.info(rep)
        rep_json = json.loads(rep)
        msg = json.loads(rep_json['message'])
        if 'errorCode' in msg:
            raise BaozunPullException("CALLING BAOZUN WITH ERROR:(%s,%s)" % (msg['errorCode'], msg['msg']))
        return req, rep_json['message']


class BaozunBatchPull(BaozunPull):
    def run(self, param):
        failed = []
        msgs = []
        for p in param['pages']:
            (req, rep) = self.service(startTime=param['startTime'], endTime=param['endTime'], page=int(p),
                                      pageSize=param['pageSize'])
            rep_json = json.loads(rep)
            msg = json.loads(rep_json['message'])
            total = msg['total']
            if 'errorCode' in msg:
                self.logger.warn(msg['msg'])
                failed.append(p)
            else:
                msgs.append(rep_json['message'])
        return msgs, failed, total


if __name__ == '__main__':
    url = 'https://hub-test.baozun.cn/web-service/warehouse/1.0?wsdl'
    # b = BaozunPullOnce(url=url, customer='WH_OCL', key='abcdef', sign='123456', order_type='SPO_SALES')
    bb = BaozunBatchPull(url=url, customer='WH_OCL', key='abcdef', sign='123456', order_type='SPO_SALES')
    param = {
        'startTime': '2018-05-10 14:00:00',
        'endTime': '2018-05-10 14:05:00',
        'pages': [1, 2, 3],
        'pageSize': 100
    }
    # (req, rep) = b.run(param)
    msgs, failed, = bb.run(param)

    pass
