from zato.server.service import Service
import json

config = {
    'url': 'https://hub-test.baozun.cn/web-service/warehouse/1.0?wsdl',
    'customer': 'WH_OCL',
    'key': 'abcdef',
    'sign': '123456',
    "dmtpServer": "http://wmsuat.oocllogistics.com",
    "dmtpUrl":
        {
            "ITEM": "/DMTP_AEServer/HttpService?profileName=GUAW0058_20024904CRR_NIKE_Inbound_Item",
            "ASN": "/DMTP_AEServer/HttpService?profileName=GUAW0058_20024904CRR_NIKE_Inbound_ASN",
            "SPO": "/DMTP_AEServer/HttpService?profileName=GUAW0058_20024904CRR_SPO_AND_SALES",
            "SPO_SALES": "/DMTP_AEServer/HttpService?profileName=GUAW0058_20024904CRR_SPO_AND_SALES",
        },
    'pull_once_ws': 'baozun-pull-once.baozun-pull-once'
}


class DMTPException(Exception):
    pass


class BaozunException(Exception):
    pass


class BaozunPullWorkerService(Service):
    class SimpleIO:
        input_required = ('startTime', 'endTime', 'pages', 'pageSize', 'type')
        output_required = ('failed',)

    def handle(self):
        startTime = self.request.input.startTime
        endTime = self.request.input.endTime
        pages = json.loads(self.request.input.pages)
        pageSize = int(self.request.input.pageSize)
        service = self.request.input.type
        self.response.payload.failed = []
        for p in pages:
            p = int(p)
            try:
                rep = self._baozunCall(startTime=startTime, endTime=endTime, page=p, pageSize=pageSize,
                                       service=service).response.message
                self._dmtpCall(config['dmtpServer'] + config['dmtpUrl'][self.request.input.service], json.dumps(rep))
                fn = '%s_%s_%d.json' % (startTime, endTime, p)
                self._write_to_file(fn=fn, data=json.dumps(rep))
            except Exception as e:
                self.logger.error(e.message)
                self.response.payload.failed.append(p)

    def _write_to_file(self, fn, data):
        self.invoke_async('logfile.log-file', {'path': '%s' % fn, 'data': data, 'type': 'json'})

    def _dmtpCall(self, url, msg):
        try:
            return self.invoke('dmtpservice.dmtp-service', {'url': url, 'data': msg})
        except Exception as e:
            raise DMTPException('CALLING DMTP WITH ERROR: %s' % e.message)

    def _baozunCall(self, startTime, endTime, page, pageSize, service):
        self.invoke(config['pull_once_ws'],
                    {'type': service, 'startTime': startTime, 'endTime': endTime, 'page': page, 'pageSize': pageSize},
                    as_bunch=True)
