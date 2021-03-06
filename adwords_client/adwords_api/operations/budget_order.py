from .utils import _get_selector


def budget_order_operation(budget_order_id: 'Long' = None,
                           billing_account_id: 'Long' = None,
                           primary_billing_id: 'Long' = None,
                           start_date_time: 'DateTime' = None,
                           end_date_time: 'DateTime' = None,
                           spending_limit: 'Money' = -1,
                           po_number: 'String' = None,
                           budget_order_name: 'String' = None,
                           time_zone: 'String' = 'America/Sao_Paulo',
                           operator: 'String' = 'ADD',
                               **kwargs):
    if not spending_limit:
        spending_limit = -1

    operation = {
        'operator': operator.upper(),
        'xsi_type': 'BudgetOrderOperation',
        'operand': {
            'billingAccountId': billing_account_id,
            'primaryBillingId': primary_billing_id,
            'startDateTime': start_date_time + ' ' + time_zone,
            'endDateTime': end_date_time + ' ' + time_zone,
            'spendingLimit':
                {
                    'microAmount': spending_limit
                }
        }
    }

    if budget_order_id:
        operation['operand']['id'] = budget_order_id
    if po_number:
        operation['operand']['poNumber'] = po_number
    if budget_order_name:
        operation['operand']['budgetOrderName'] = budget_order_name
    return operation


def get_budget_order_operation(fields=[], predicates=[], **kwargs):
    default_fields = kwargs.pop('default_fields', False)
    object_type = kwargs.pop('object_type', None)
    if object_type == 'billing_account':
        return {}
    if default_fields:
        fields = set(fields).union({'Id', 'BudgetOrderName'})
    return _get_selector(fields, predicates)
