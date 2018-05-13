from zato.server.service import Service


class LockService(object):
    class ServiceLockedException(Exception):
        pass

    def __init__(self, service):
        self.service = service
        self.suc = False

    def __enter__(self):
        self.suc = self.service.kvdb.conn.setnx(self.service.name, 'lock')
        if not self.suc:
            raise LockService.ServiceLockedException('%s is locked' % self.service.name)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.suc:
            self.service.kvdb.conn.delete(self.service.name)


class BaozunCronPull:
    def __init__(self, service):
        self.service = service

    def run(self,startTime,endTime,pageSize):
        try:
            with LockService(self.service):
                self.service.
                pass
        except Exception as e:
            self.service.logger.warn(e.message)


class PullASN(Service):
    def handle(self):
        pass


class PullITEM(Service):
    def handle(self):
        pass


class PullSPO(Service):
    def handle(self):
        pass


class PullSPOSALES(Service):
    def handle(self):
        pass
