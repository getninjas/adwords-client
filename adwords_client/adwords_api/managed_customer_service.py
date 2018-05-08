from . import common as cm


class ManagedCustomerService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'ManagedCustomerService')

    #Samuel, vc ainda usa isso?

    # def get_customers(self, client_customer_id=None, predicate=None):
    #     self.prepare_get()
    #     self.helper.add_fields('CustomerId', 'Name')
    #     if predicate:
    #         for predicate_item in predicate:
    #             self.helper.add_predicate(predicate_item['field'], predicate_item['operator'], predicate_item['values'])
    #     client_id = client_customer_id if client_customer_id else self.client.client_customer_id
    #     for customer in self.get(client_id):
    #         yield customer
