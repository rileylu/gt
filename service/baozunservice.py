from abc import ABCMeta, abstractmethod
from com.oocl.gt.baozun.baozunws import BaozunWebService


class BaozunService:
    __metaclass__ = ABCMeta

    def __init__(self, service):
        config = {
            'url': service.kvdb.conn.get('baozun_url'),
            'customer': service.kvdb.conn.get('baozun_customer'),
            'key': service.kvdb.conn.get('baozun_key'),
            'sign': service.kvdb.conn.get('baozun_sign')
        }
        self.service = service
        self.baozun_ws = BaozunWebService(config['url'], config['customer'], config['key'], config['sign'])

    def run(self, fun):
        try:
            self.do_job(fun)
        except Exception as e:
            self.service.logger.error(e.message)

    @abstractmethod
    def do_job(self, fun):
        pass


class BaozunLockedService(BaozunService):

    def do_job(self, fun):
        from com.oocl.gt.util.servicelock import ServiceLock
        with ServiceLock(self.service):
            fun()


class BaozunUnlockedService(BaozunService):
    def do_job(self, fun):
        fun()

class BaozunPullService(BaozunService):
    def do_job(self, fun):
        pass