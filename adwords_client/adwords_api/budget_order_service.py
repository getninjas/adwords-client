from . import common as cm


class BudgetOrderService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'BudgetOrderService')

    def get(self, operation, client_id=None):
        if client_id:
            self.client.SetClientCustomerId(client_id)
        op = next(operation)
        if op == {}:
            return self.service.getBillingAccounts()
        self.ResultProcessor = cm.PagedResult
        return self.ResultProcessor(self.service.get, op)
