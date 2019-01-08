from . import common as cm


class ManagedCustomerService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'ManagedCustomerService')

    # def mutate(self, client_customer_id=None, sync=None):
    #     if client_customer_id:
    #         self.client.SetClientCustomerId(client_customer_id)
    #     result = self.service.mutate(self.helper.operations)
    #     for item in result.value:
    #         item['returnType'] = 'ManagedCustomer'
    #     return result.value
