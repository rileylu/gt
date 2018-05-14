from zato.server.service import Service
from zato.server.service import ZatoException


class BaozunException(ZatoException):
    pass


class TestService(Service):
    def handle(self):
        raise BaozunException(msg='hehe')


class TestClientService(Service):
    def handle(self):
        try:
            # self.invoke('baozun-pull-once.baozun-pull-once')
            self.invoke('test.test-service')
        except ZatoException, e:
            self.logger.error(e.message)
