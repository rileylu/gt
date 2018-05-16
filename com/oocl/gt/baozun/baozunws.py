from requests import Session
from zeep import CachingClient
from zeep.transports import Transport
from Crypto.Cipher import AES
import hashlib
import base64
import logging

BLOCK_SIZE = 16
pad = lambda s: s + (BLOCK_SIZE - len(s) % BLOCK_SIZE) * \
                chr(BLOCK_SIZE - len(s) % BLOCK_SIZE)
unpad = lambda s: s[:-ord(s[len(s) - 1:])]


def md5(str):
    return hashlib.md5(str).hexdigest()


class BaozunWSProxy:
    def __init__(self, url):
        s = Session()
        s.verify = False
        self.client = CachingClient(wsdl=url, transport=Transport(session=s))

    def wmsInBoundOrder(self, customer, sign, message):
        return self.client.service.wmsInBoundOrder(customer=customer, sign=sign, message=message)

    def wmsOutBoundOrder(self, customer, sign, message):
        return self.client.service.wmsOutBoundOrder(customer=customer, sign=sign, message=message)

    def wmsSku(self, customer, sign, message):
        return self.client.service.wmsSku(customer=customer, sign=sign, message=message)

    def wmsSalesOrder(self, customer, sign, message):
        return self.client.service.wmsSalesOrder(customer=customer, sign=sign, message=message)

    def uploadWmsInBoundOrder(self, customer, sign, message):
        return self.client.service.uploadWmsInBoundOrder(customer=customer, sign=sign, message=message)

    def uploadWmsInvChange(self, customer, sign, message):
        return self.client.service.uploadWmsInvChange(customer=customer, sign=sign, message=message)

    def uploadWmsInvStatusChange(self, customer, sign, message):
        return self.client.service.uploadWmsInvStatusChange(customer=customer, sign=sign, message=message)

    def uploadWmsSalesOrder(self, customer, sign, message):
        return self.client.service.uploadWmsSalesOrder(customer=customer, sign=sign, message=message)

    def uploadWmsOutBoundOrder(self, customer, sign, message):
        return self.client.service.uploadWmsOutBoundOrder(customer=customer, sign=sign, message=message)


class BaozunWebService:
    class BaozunCrypt:
        def __init__(self, key):
            self.key = md5(key)[8:24]
            self.mode = AES.MODE_ECB

        def encrypt(self, text):
            text = pad(text)
            generator = AES.new(self.key, self.mode)
            s = base64.b64encode(generator.encrypt(text))
            return s.decode()

        def decrypt(self, text):
            text += (len(text) % 4) * '='
            text = base64.b64decode(text)
            generator = AES.new(self.key, self.mode)
            return unpad(generator.decrypt(text))

    def __init__(self, url, cus, key, sign):
        self.proxy = BaozunWSProxy(url)
        self.cus = cus
        self.key = key
        self.sign = sign
        self.crypt = BaozunWebService.BaozunCrypt(key)
        self.logger = logging.getLogger('repreq')

    def _do_pull(self, startTime, endTime, page, pageSize, fun):
        import json
        data = {'startTime': startTime, 'endTime': endTime, 'page': page, 'pageSize': pageSize}
        data = json.dumps(data)
        sign = md5('%s%s%s' % (self.cus, data, self.sign))
        message = self.crypt.encrypt(data)
        self.logger.info(message)
        r = fun(customer=self.cus, sign=sign, message=message)
        self.logger.info(r)
        return data, self.crypt.decrypt(r)

    def _do_push(self, data, fun):
        data = str(data).encode('unicode_escape')
        sign = md5('%s%s%s' % (self.cus, data, self.sign))
        message = self.crypt.encrypt(data)
        self.logger.info(message)
        r = fun(customer=self.cus, sign=sign, message=message)
        self.logger.info(r)
        return data, self.crypt.decrypt(r)

    def pull_asn(self, startTime, endTime, page, pageSize):
        return self._do_pull(startTime, endTime, page, pageSize, self.proxy.wmsInBoundOrder)

    def pull_spo(self, startTime, endTime, page, pageSize):
        return self._do_pull(startTime, endTime, page, pageSize, self.proxy.wmsOutBoundOrder)

    def pull_sku(self, startTime, endTime, page, pageSize):
        return self._do_pull(startTime, endTime, page, pageSize, self.proxy.wmsSku)

    def pull_sales_order(self, startTime, endTime, page, pageSize):
        return self._do_pull(startTime, endTime, page, pageSize, self.proxy.wmsSalesOrder)

    def push_gr(self, data):
        return self._do_push(data, self.proxy.uploadWmsInBoundOrder)

    def push_do(self, data):
        return self._do_push(data, self.proxy.uploadWmsOutBoundOrder)

    def push_sales_do(self, data):
        return self._do_push(data, self.proxy.uploadWmsSalesOrder)

    def push_inv_change(self, data):
        return self._do_push(data, self.proxy.uploadWmsInvChange)

    def push_inv_status_change(self, data):
        return self._do_push(data, self.proxy.uploadWmsInvStatusChange)


if __name__ == '__main__':
    import time
    import json

    url = 'https://hub-test.baozun.cn/web-service/warehouse/1.0?wsdl'
    b = BaozunWebService(url=url, cus='WH_OCL', key='abcdef', sign='123456')
    start = time.time()
    ##################pull####################
    # (req, rep) = b.pull_sku('2018-04-10 00:00:00', '2018-05-10 14:00:00', 1, 50)
    # (req,rep)=b.pull_asn('2018-04-10 00:00:00','2018-05-10 14:00:00',1,50)
    # (req,rep)=b.pull_spo('2018-05-01 00:00:00','2018-05-10 14:00:00',1,50)
    try:
        (req, rep) = b.pull_sales_order('2018-05-01 13:55:00', '2018-05-11 14:00:00', 1, 50)
        rep = json.loads(rep)['message']
        if 'errorCode' in rep:
            print rep['msg']
        pass
    except Exception as e:
        print e.message

    ##################push###################
    # gr = r'{"warehouseCode": "WH_OOCL", "uuid": "1", "orderCode": "", "lines": [{"qty": 2, "invStatus": "accepted", "skuCode": ""}, {"qty": 2, "invStatus": "accepted", "skuCode": ""}, {"qty": 2, "invStatus": "accepted", "skuCode": ""}, {"qty": 2, "invStatus": "accepted", "skuCode": ""}, {"qty": 2, "invStatus": "accepted", "skuCode": ""}, {"qty": 2, "invStatus": "accepted", "skuCode": ""}, {"qty": 2, "invStatus": "accepted", "skuCode": ""}, {"qty": 2, "invStatus": "accepted", "skuCode": ""}], "inboundTime": "20180507141701", "extMemo": "", "type": "1"}'
    # (req, rep) = b.push_gr(data)
    # (req, rep) = b.push_inv_change(data)
    # (req, rep) = b.push_inv_status_change(gr)data

    do = r'{"warehouseCode": "WH_OCL", "uuid": "1", "orderCode": "R600087181359", "trackingNo": "66655555", "lines": [{"skuCode": "nike49151918999", "cartonNo": "", "expDate": "", "qty": 1, "invStatus": "accepted", "extMemo": ""}], "snLines": [], "lpCode": "SF", "extMemo": "", "outboundTime": "2018-05-02 14:11:55", "type": "10"}'
    do_sales = r'{"transNos": "", "warehouseCode": "WH_OCL", "uuid": "1", "orderCode": "S600087181165", "trackingNo": "444034512959", "lines": [{"extMemo": "", "qty": 1, "invStatus": "accepted", "expDate": "", "skuCode": "nike49151918999"}], "weight": 25.3, "materialSkus": "", "snLines": [], "lpCode": "SF", "extMemo": "", "outboundTime": "2018-05-02 11:41:01", "type": "21"}'

    # (req, rep) = b.push_do(do)
    # (req, rep) = b.push_sales_do(do_sales)

    end = time.time()
    print end - start
    # b = BaozunWebService(url=url, cus='WH_OCL', key='abcdef', sign='123456')
    # start = time.time()
    # x = b.get_spo_sales('2018-05-10 00:00:00', '2018-05-10 19:00:00', 100, 50)
    # end = time.time()
    # print x
    # print end - start
    pass
