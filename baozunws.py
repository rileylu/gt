from requests import Session
from zeep import CachingClient
from zeep.transports import Transport
from Crypto.Cipher import AES
import hashlib
import base64

BLOCK_SIZE = 16
pad = lambda s: s + (BLOCK_SIZE - len(s) % BLOCK_SIZE) * \
                chr(BLOCK_SIZE - len(s) % BLOCK_SIZE)
unpad = lambda s: s[:-ord(s[len(s) - 1:])]


def md5(str):
    return hashlib.md5(str).hexdigest()


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
        s = Session()
        s.verify = False
        t = Transport(session=s)
        self.client = CachingClient(wsdl=url, transport=t)
        self.cus = cus
        self.key = key
        self.sign = sign
        self.crypt = BaozunWebService.BaozunCrypt(key)

    def _do_pull(self, startTime, endTime, page, pageSize, fun):
        import json
        data = {'startTime': startTime, 'endTime': endTime, 'page': page, 'pageSize': pageSize}
        data = json.dumps(data)
        sign = md5('%s%s%s' % (self.cus, data, self.sign))
        message = self.crypt.encrypt(data)
        r = fun(customer=self.cus, sign=sign, message=message)
        return self.crypt.decrypt(r)

    def _do_push(self, data, fun):
        data = str(data).encode('unicode_escape')
        sign = md5('%s%s%s' % (self.cus, data, self.sign))
        message = self.crypt.encrypt(data)
        r = fun(customer=self.cus, sign=sign, message=message)
        return self.crypt.decrypt(r)

    def pull_asn(self, startTime, endTime, page, pageSize):
        r = self._do_pull(startTime=startTime, endTime=endTime, page=page, pageSize=pageSize,
                          fun=self.client.service.wmsInBoundOrder)
        return r

    def pull_spo(self, startTime, endTime, page, pageSize):
        r = self._do_pull(startTime=startTime, endTime=endTime, page=page, pageSize=pageSize,
                          fun=self.client.service.wmsOutBoundOrder)
        return r

    def pull_item(self, startTime, endTime, page, pageSize):
        r = self._do_pull(startTime=startTime, endTime=endTime, page=page, pageSize=pageSize,
                          fun=self.client.service.wmsSku)
        return r

    def pull_spo_sales(self, startTime, endTime, page, pageSize):
        r = self._do_pull(startTime=startTime, endTime=endTime, page=page, pageSize=pageSize,
                          fun=self.client.service.wmsSalesOrder)
        return r

    def push_gr(self, data):
        r = self._do_push(data=data, fun=self.client.service.uploadWmsInBoundOrder)
        return r

    def push_inv_change(self, data):
        r = self._do_push(data=data, fun=self.client.service.uploadWmsInvChange)
        return r

    def push_inv_status(self, data):
        r = self._do_push(data=data, fun=self.client.service.uploadWmsInvStatusChange)
        return r

    def push_do_sales(self, data):
        r = self._do_push(data=data, fun=self.client.service.uploadWmsSalesOrder)
        return r

    def push_do(self, data):
        r = self._do_push(data=data, fun=self.client.service.uploadWmsOutBoundOrder)
        return r


if __name__ == '__main__':
    import time

    url = 'https://hub-test.baozun.cn/web-service/warehouse/1.0?wsdl'
    b = BaozunWebService(url=url, cus='WH_OCL', key='abcdef', sign='123456')
    start = time.time()
    x = b.get_spo_sales('2018-05-10 00:00:00', '2018-05-10 19:00:00', 100, 50)
    end = time.time()
    print x
    print end - start
    pass
