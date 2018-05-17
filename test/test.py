# from zato.server.service import Service
# from zato.server.service import ZatoException
#
#
# class BaozunException(ZatoException):
#     pass
#
#
# class TestService(Service):
#     def handle(self):
#         raise BaozunException(msg='hehe')
#
#
# class TestClientService(Service):
#     def handle(self):
#         try:
#             # self.invoke('baozun-pull-once.baozun-pull-once')
#             self.invoke('test.test-service')
#         except ZatoException, e:
#             self.logger.error(e.message)

import time
import random
import threading


def run():
    i = 1
    while i < 10:
        time.sleep(random.random())
        print threading.current_thread().name, i
        i += 1


if __name__ == '__main__':
    from concurrent import futures

    with futures.ThreadPoolExecutor(max_workers=None) as exe:
        fds = []
        fds.append(exe.submit(fn=run))
        fds.append(exe.submit(fn=run))
        fds.append(exe.submit(fn=run))
    print "=================================================================================="
