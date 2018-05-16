import logging
import json


class BaozunPushException(Exception):
    pass


class BaozunPush:
    def __init__(self, baozun_ws, order_type):
        service = {
            'GR': baozun_ws.push_gr,
            'INV_CHANGE': baozun_ws.push_inv_change,
            'INV_STATUS': baozun_ws.push_inv_status_change,
            'DO': baozun_ws.push_do,
            'DO_SALES': baozun_ws.push_sales_do
        }
        if order_type not in service:
            raise BaozunPushException("ORDER_TYPE %s IS NOT DEFINED")
        self.service = service[order_type]
        self.logger = logging.getLogger('repreq.%s' % order_type)

    def run(self, data):
        (req, rep) = self.service(data)
        self.logger.info(req)
        self.logger.info(rep)
        rep_json = json.loads(rep)
        msg = json.loads(rep_json['message'])
        if 'errorCode' in msg:
            raise BaozunPushException("CALLING BAOZUN WITH ERROR:(%s,%s)" % (msg['errorCode'], msg['msg']))
        return req,rep

if __name__ == '__main__':
    from com.oocl.gt.baozun.baozunws import BaozunWebService
    url = 'https://hub-test.baozun.cn/web-service/warehouse/1.0?wsdl'
    b = BaozunWebService(url=url, cus='WH_OCL', key='abcdef', sign='123456')
    bp=BaozunPush(b,'GR')
    ##################push###################
    gr = r'{"warehouseCode": "WH_OOCL", "uuid": "1", "orderCode": "", "lines": [{"qty": 2, "invStatus": "accepted", "skuCode": ""}, {"qty": 2, "invStatus": "accepted", "skuCode": ""}, {"qty": 2, "invStatus": "accepted", "skuCode": ""}, {"qty": 2, "invStatus": "accepted", "skuCode": ""}, {"qty": 2, "invStatus": "accepted", "skuCode": ""}, {"qty": 2, "invStatus": "accepted", "skuCode": ""}, {"qty": 2, "invStatus": "accepted", "skuCode": ""}, {"qty": 2, "invStatus": "accepted", "skuCode": ""}], "inboundTime": "20180507141701", "extMemo": "", "type": "1"}'
    # (req, rep) = b.push_gr(data)
    # (req, rep) = b.push_inv_change(data)
    # (req, rep) = b.push_inv_status_change(gr)data

    # do = r'{"warehouseCode": "WH_OCL", "uuid": "1", "orderCode": "R600087181359", "trackingNo": "66655555", "lines": [{"skuCode": "nike49151918999", "cartonNo": "", "expDate": "", "qty": 1, "invStatus": "accepted", "extMemo": ""}], "snLines": [], "lpCode": "SF", "extMemo": "", "outboundTime": "2018-05-02 14:11:55", "type": "10"}'
    bp.run(gr)
    pass
    # do_sales = r'{"transNos": "", "warehouseCode": "WH_OCL", "uuid": "1", "orderCode": "S600087181165", "trackingNo": "444034512959", "lines": [{"extMemo": "", "qty": 1, "invStatus": "accepted", "expDate": "", "skuCode": "nike49151918999"}], "weight": 25.3, "materialSkus": "", "snLines": [], "lpCode": "SF", "extMemo": "", "outboundTime": "2018-05-02 11:41:01", "type": "21"}'

    # (req, rep) = b.push_do(do)
    # (req, rep) = b.push_sales_do(do_sales)