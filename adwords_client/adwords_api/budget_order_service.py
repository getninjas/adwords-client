from . import common as cm


class BudgetOrderService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'BudgetOrderService')

    def sync_get(self, operation, client_id=None):
        self.ResultProcessor = cm.PagedResult
        if client_id:
            self.client.SetClientCustomerId(client_id)
        op = next(operation)
        if op == {}:
            return self.service.getBillingAccounts()
        return self.ResultProcessor(self.service.get, op)
