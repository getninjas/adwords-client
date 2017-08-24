from . import common as cm


class ManagedCustomerService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'ManagedCustomerService')

    def get_customers(self, client_customer_id=None):
        self.prepare_get()
        self.helper.add_fields('CustomerId', 'Name')
        client_id = client_customer_id if client_customer_id else self.client.client_customer_id
        for customer in self.get(client_id):
            yield customer
