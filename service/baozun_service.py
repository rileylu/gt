# from com.oocl.gt.util.servicelock import *
# from com.oocl.gt.baozun.baozunws import *
# from zato.server.service import Service
#
# dt_format = '%Y-%m-%d %H:%M:%S'
#
#
# class BaozunService(Service):
#     def do_job(self):
#         raise NotImplementedError("Not Implemented")
#
#
# class BaozunLockedService(BaozunService):
#     def handle(self):
#         try:
#             with ServiceLock(self):
#                 config = {
#                     'url': self.kvdb.conn.get('baozun_url'),
#                     'customer': self.kvdb.conn.get('baozun_customer'),
#                     'key': self.kvdb.conn.get('baozun_key'),
#                     'sign': self.kvdb.conn.get('baozun_sign')
#                 }
#                 self.baozun_ws = BaozunWebService(url=config['url'], cus=config['customer'], key=config['key'],
#                                                   sign=config['sign'])
#                 self.do_job()
#         except Exception as e:
#             self.logger.error(e.message)
#
#
# class BaozunUnlockedService(Service):
#     def handle(self):
#         try:
#             config = {
#                 'url': self.kvdb.conn.get('baozun_url'),
#                 'customer': self.kvdb.conn.get('baozun_customer'),
#                 'key': self.kvdb.conn.get('baozun_key'),
#                 'sign': self.kvdb.conn.get('baozun_sign')
#             }
#             self.baozun_ws = BaozunWebService(url=config['url'], cus=config['customer'], key=config['key'],
#                                               sign=config['sign'])
#             self.do_job()
#         except Exception as e:
#             self.logger.error(e.message)
#
#
# class BaozunPullService(BaozunService):
#     def do_job(self):
#         import datetime
#         (startTime, endTime, pageSize, fun) = self.get_config()
#         s = datetime.datetime.strptime(startTime, dt_format)
#         e = datetime.datetime.strptime(endTime, dt_format)
#         cur = s
#         while cur < e:
#             (req, rep) = fun(startTime=cur.strftime(dt_format),
#                              endTime=(cur + datetime.timedelta(minutes=5)).strftime(dt_format),
#                              page=1, pageSize=pageSize)
#             msg = self._parse_rep(rep)
#             total = msg['total']
#             gots = len(msg['entry'])
#             page = 2
#             while gots < total:
#                 (req, rep) = fun(startTime=cur.strftime(dt_format),
#                                  endTime=(cur + datetime.timedelta(minutes=5)).strftime(dt_format),
#                                  page=page, pageSize=pageSize)
#                 msg = self._parse_rep(rep)
#                 gots = gots + len(msg['entry'])
#                 page += 1
#
#     def get_config(self):
#         raise NotImplementedError("Not Implemented")
#
#     def _do_request(self, startTime, endTime, page, pageSize):
#         pass
#
#     def _parse_rep(self, rep):
#         import json
#         rep_json = json.loads(rep)
#         msg = json.loads(rep_json['message'])
#         return msg
#
#
# class BaozunPullASN(BaozunUnlockedService):
#     def do_job(self):
#         pass
