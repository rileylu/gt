from com.oocl.gt.baozun.baozunws import BaozunWebService
from zato.server.service import Service
from math import ceil
from math import floor
import logging
import json

dt_format = '%Y-%m-%d %H:%M:%S'
fn_format = '%Y%m%d%H%M%S'

config = {
    'url': 'https://hub-test.baozun.cn/web-service/warehouse/1.0?wsdl',
    'customer': 'WH_OCL',
    'key': 'abcdef',
    'sign': '123456',
    'startTime': '2018-05-10 14:00:00',
    'endTime': '2018-05-10 23:59:59',
    'pageSize': 100,
    'thread_count': 10,
    "dmtpServer": "http://wmsuat.oocllogistics.com",
    "dmtpUrl": "/DMTP_AEServer/HttpService?profileName=GUAW0058_20024904CRR_SPO_AND_SALES"
}


class ServiceLockedException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)


class LockService(object):
    def __init__(self, service):
        self.service = service
        self.suc = False

    def __enter__(self):
        self.suc = self.service.kvdb.conn.setnx(self.service.name, 1)
        if not self.suc:
            raise ServiceLockedException('%s is locked' % self.service.name)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.suc:
            self.service.kvdb.conn.delete(self.service.name)


class BaozunRageBasedPull(Service):
    def handle(self):
        try:
            with LockService(self):
                self.repreq = logging.getLogger('repreq')
                self.logger.info("---starting: %s ---" % self.name)
                self.ws = BaozunWebService(config['url'], config['customer'], config['key'], config['sign'])
                self.do_job()
        except ServiceLockedException as e:
            self.logger.warning(e.message)
        except Exception as e:
            self.logger.error(e.message)

    def do_job(self):
        import datetime
        startTime = config['startTime']
        endTime = config['endTime']
        pageSize = config['pageSize']

        s = datetime.datetime.strptime(startTime, dt_format)
        e = datetime.datetime.strptime(endTime, dt_format)
        cur = s
        # every time range
        while cur < e:
            st = cur
            et = cur + datetime.timedelta(minutes=5)
            fn = '%s-%s' % (st.strftime(fn_format), et.strftime(fn_format))
            total=0
            try:
                (req, rep) = self.ws.pull_sales_order(startTime=st.strftime(dt_format),
                                                  endTime=et.strftime(dt_format),
                                                  page=1, pageSize=pageSize)
                msg = self._parse_rep(rep)
                if 'errorCode' in msg:
                    self.logger.error(msg['msg'])
                    continue
                total = msg['total']
                self.logger.info('total:%d' % total)
            except Exception as e:
                self.logger.error(e.message)
                continue
            if 0 < total < pageSize * config['thread_count']:
                self._run_once(fn, st, et, 1, int(ceil(total / pageSize)), pageSize)
            elif total >= pageSize * config['thread_count']:
                ttlPages = int(ceil(total / pageSize))
                step = int(floor(ttlPages / config['thread_count']))
                x = range(1, ttlPages, step)
                import threading
                tds = []
                i = 1
                while i < len(x):
                    td = threading.Thread(target=self._run_once, args=(fn, st, et, x[i - 1], x[i], pageSize))
                    td.start()
                    tds.append(td)
                    i += 1
                if x[-1] < ttlPages:
                    td = threading.Thread(target=self._run_once, args=(fn, st, et, x[-1], ttlPages+1, pageSize))
                    td.start()
                    tds.append(td)
                for td in tds:
                    td.join()
            cur += datetime.timedelta(minutes=5)

    def _run_once(self, fn, st, et, ps, pe, pageSize):
        i = ps
        while i < pe:
            try:
                (req, rep) = self.ws.pull_sales_order(startTime=st.strftime(dt_format),
                                                      endTime=et.strftime(dt_format),
                                                      page=i, pageSize=pageSize)
                self.repreq.info(req)
                self.repreq.info(rep)
                msg = self._parse_rep(rep)
                if 'errorCode' in msg:
                    self.logger.error(msg['msg'])
                    continue
                self._dmtpCall(config['dmtpServer'] + config['dmtpUrl'], json.dumps(msg))
                self._write_to_file(fn, i, msg)
                self.logger.info('%s_%d' % (fn, i))
            except Exception as e:
                self.logger.error(e)
                continue
            i += 1

    def _parse_rep(self, rep):
        import json
        rep_json = json.loads(rep)
        msg = json.loads(rep_json['message'])
        return msg

    def _write_to_file(self, fn, page, data):
        self.invoke_async('logfile.log-file', {'path': '%s_%d' % (fn, page), 'data': data, 'type': 'json'})

    def _dmtpCall(self, url, msg):
        return self.invoke('dmtpservice.dmtp-service', {'url': url, 'data': msg})
