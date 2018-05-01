from . import common as cm


class BudgetOrderService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'BudgetOrderService')

    def custom_get(self, internal_operation):
        client_id = internal_operation.get('client_id')
        if internal_operation['object_type'] == 'billing_account':
            if client_id is not None:
                self.client.SetClientCustomerId(client_id)
            return self.service.getBillingAccounts()
        else:
            fields = ['Id', 'Name']
            self.prepare_get()
            for predicate_item in internal_operation.get('predicate', []):
                self.helper.add_predicate(predicate_item['field'], predicate_item['operator'], predicate_item['values'])
            fields = set(fields).union(internal_operation.get('fields', []))
            self.helper.add_fields(*fields)
            return self.get(client_id)
