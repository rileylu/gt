from zato.server.service import Service


class BaozunException(Exception):
    pass


class TestService(Service):
    class SimpleIO:
        input_required = ('startTime', 'endTime', 'pages', 'pageSize', 'type')

    def handle(self):
        import json
        self.logger.info(self.request.input.startTime)
        self.logger.info(self.request.input.endTime)
        self.logger.info(self.request.input.pageSize)
        self.logger.info(self.request.input.type)
        r = json.loads(self.request.input.pages)
        for x in r:
            self.logger.info(x)


class TestClientService(Service):
    def handle(self):
        t = {
            "type": "ASN",
            "startTime": "2018-05-01 00:00:00",
            "endTime": "2018-05-10 00:00:00",
            "pageSize": 100,
            "page": 1
        }
        try:
            self.invoke('baozun-pull-once.baozun-pull-once', t, as_bunch=True)
        except Exception as e:
            pass
