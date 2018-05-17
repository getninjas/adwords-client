from . import common as cm


class BudgetOrderService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'BudgetOrderService')

    def get(self, operation, client_id=None):
        ops = list(operation)
        op = ops[0]
        if not op:
            return self.service.getBillingAccounts()
        return super().get([op], client_id)

