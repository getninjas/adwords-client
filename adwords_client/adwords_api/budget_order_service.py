from . import common as cm


class BudgetOrderService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'BudgetOrderService')

    def cs_mutate(self, customer_id, operations):
        self.prepare_mutate()
        self.helper.add_operations(operations)
        return self.mutate(customer_id)

    def cs_get(self, internal_operation):
        client_id = None
        if 'client_id' in internal_operation:
            client_id = internal_operation['client_id']
        if internal_operation['object_type'] == 'billing_account':
            if client_id is not None:
                self.client.SetClientCustomerId(client_id)
            return self.service.getBillingAccounts()
        else:
            fields = ['Id', 'Name']
            self.prepare_get()
            if 'predicate' in internal_operation:
                for predicate_item in internal_operation['predicate']:
                    self.helper.add_predicate(predicate_item['field'], predicate_item['operator'], predicate_item['values'])
            if 'fields' in internal_operation:
                [fields.append(field) for field in internal_operation['fields'] if field not in fields]
            self.helper.add_fields(*fields)
            return self.get(client_id)
