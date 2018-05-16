from com.oocl.gt.baozun.baozunws import BaozunWebService
from com.oocl.gt.baozun.baozunpush import BaozunPush
from zato.server.service import Service
from datetime import datetime
import json

fn_format = '%Y%m%d%H%M%S'
logfile_service = 'logfile.log-file'

config = {
    'url': 'https://hub-test.baozun.cn/web-service/warehouse/1.0?wsdl',
    'customer': 'WH_OCL',
    'key': 'abcdef',
    'sign': '123456'
}


class BaozunPushExecutor(Service):
    class SimpleIO:
        input_required = ('order_type',)

    def handle(self):
        baozunWS = BaozunWebService(url=config['url'], cus=config['customer'], key=config['key'], sign=config['sign'])
        bp = BaozunPush(baozunWS, self.request.input.order_type)
        try:
            payload = json.dumps(self.request.payload)
            (req, rep) = bp.run(payload)
            self._write_to_file(datetime.now().strftime(fn_format), self.request.input.order_type, rep)
        except Exception, e:
            self.logger.warn(e.message)

    def _write_to_file(self, dt, orderType, data):
        data = json.dumps(json.loads(data))
        self.invoke_async(logfile_service,
                          {'path': '%s_%s' % (orderType, dt), 'data': data, 'type': 'json'})
