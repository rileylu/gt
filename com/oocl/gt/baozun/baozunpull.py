from abc import abstractmethod, ABCMeta
import logging
import json


class BaozunPullException(Exception):
    pass


class BaozunPull:
    __metaclass__ = ABCMeta

    def __init__(self, baozunWS, order_type):
        self._baozunWS = baozunWS
        service = {
            "ASN": self._baozunWS.pull_asn,
            "ITEM": self._baozunWS.pull_sku,
            "SPO": self._baozunWS.pull_spo,
            "SPO_SALES": self._baozunWS.pull_sales_order
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
            self.logger.info('<startTime:%s , endTime:%s , page:%d , pageSize:%d>'%(param['startTime'],param['endTime'],p,param['pageSize']))
            (req, rep) = self.service(startTime=param['startTime'], endTime=param['endTime'], page=int(p),
                                      pageSize=param['pageSize'])
            rep_json = json.loads(rep)
            msg = json.loads(rep_json['message'])
            if 'errorCode' in msg:
                self.logger.warn(msg['msg'])
                failed.append(p)
            else:
                msgs.append(rep_json['message'])
        return msgs, failed


if __name__ == '__main__':
    from com.oocl.gt.baozun.baozunws import BaozunWebService

    url = 'https://hub-test.baozun.cn/web-service/warehouse/1.0?wsdl'
    baozunWS = BaozunWebService(url=url, cus='WH_OCL', key='abcdef', sign='123456')
    # b = BaozunPullOnce(url=url, customer='WH_OCL', key='abcdef', sign='123456', order_type='SPO_SALES')
    bb = BaozunBatchPull(baozunWS=baozunWS, order_type='SPO_SALES')
    param = {
        'startTime': '2018-05-10 14:00:00',
        'endTime': '2018-05-10 14:05:00',
        'pages': [1],
        'pageSize': 1
    }
    # (req, rep) = b.run(param)
    msgs, failed = bb.run(param)

    pass
