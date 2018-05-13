class LockException(Exception):
    pass


class ServiceLock:
    def __init__(self, service):
        self.locked = False
        self.service = service

    def __enter__(self):
        self.locked = self.service.kvdb.conn.setnx(self.service.name, 'locked')
        if not self.locked:
            raise LockException('% is locked.' % self.service.name)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.locked:
            self.service.kvdb.conn.delete(self.service.name)
