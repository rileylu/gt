from com.oocl.gt.baozun.baozunws import BaozunWebService
from zato.server.service import Service
from math import ceil
from math import floor

dt_format = '%Y-%m-%d %H:%M:%S'

config = {
    'url': 'https://hub-test.baozun.cn/web-service/warehouse/1.0?wsdl',
    'customer': 'WH_OCL',
    'key': 'abcdef',
    'sign': '123456',
    'startTime': '2018-05-10 00:00:00',
    'endTime': '2018-05-10 23:59:59',
    'pageSize': 100,
    'thread_count': 10
}


class BaozunRageBasedPull(Service):
    def handle(self):
        self.ws = BaozunWebService(config['url'], config['customer'], config['key'], config['sign'])
        self.ws.proxy.client.wsdl.dump()
        self.do_job()

    def do_job(self):
        import datetime
        startTime = config['startTime']
        endTime = config['endTime']
        pageSize = config['pageSize']

        s = datetime.datetime.strptime(startTime, dt_format)
        e = datetime.datetime.strptime(endTime, dt_format)
        cur = s
        fno = 0
        # every time range
        while cur < e:
            fno += 1
            st = cur
            et = cur + datetime.timedelta(minutes=5)
            (req, rep) = self.ws.pull_sales_order(startTime=st.strftime(dt_format),
                                                  endTime=et.strftime(dt_format),
                                                  page=1, pageSize=pageSize)
            msg = self._parse_rep(rep)
            total = msg['total']
            self.logger.info('total:%d' % total)
            if 0 < total < pageSize * config['thread_count']:
                self._run_once(st, et, 1, int(ceil(total / pageSize)), pageSize)
            elif total > pageSize * config['thread_count']:
                ttlPages = int(ceil(total / pageSize))
                step = int(floor(ttlPages / config['thread_count']))
                x = range(1, ttlPages, step)
                import threading
                tds = []
                i = 1
                while i < len(x):
                    td = threading.Thread(target=self._run_once, args=(fno, st, et, x[i - 1], x[i], pageSize))
                    td.start()
                    tds.append(td)
                    i += 1
                if x[-1] < ttlPages:
                    td = threading.Thread(target=self._run_once, args=(fno, st, et, x[-1], ttlPages, pageSize))
                    td.start()
                    tds.append(td)
                for td in tds:
                    td.join()
            cur += datetime.timedelta(minutes=5)

    def _run_once(self, fno, st, et, ps, pe, pageSize):
        i = ps
        while i < pe:
            try:
                (req, rep) = self.ws.pull_sales_order(startTime=st.strftime(dt_format),
                                                      endTime=et.strftime(dt_format),
                                                      page=i, pageSize=pageSize)
                msg = self._parse_rep(rep)
                self._write_to_file(fno, i, msg)
                self.logger.info('page:%d' % i)
            except:
                continue
            i += 1

    def _parse_rep(self, rep):
        import json
        rep_json = json.loads(rep)
        msg = json.loads(rep_json['message'])
        return msg

    def _write_to_file(self, fno, page, data):
        import json
        with open('/opt/zato/Download/file%d_%d.json' % (fno, page), "w") as outfile:
            json.dump(data, outfile, indent=4)
