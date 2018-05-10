from .utils import _get_selector


def managed_customer_operation(client_id: 'Long' = None,
                               name: 'String' = None,
                               currency_code: 'String' = None,
                               operator: 'String' = 'ADD',
                               date_time_zone: 'String' = None,
                               **kwargs):
    operation = {
        'operator': operator.upper(),
        'operand': {
        }
    }
    if currency_code:
        operation['operand']['currencyCode'] = currency_code
    if date_time_zone:
        operation['operand']['dateTimeZone'] = date_time_zone
    if name:
        operation['operand']['name'] = name
    if client_id:
        operation['operand']['customerId'] = client_id
        operation['operator'] = 'SET'
    return operation


def managed_customer_label_operation(client_id: 'Long' = None,
                                     operator: 'String' = 'ADD',
                                     label_id: 'Long' = None,
                                     **kwargs):
    operation = {
        'operator': operator.upper(),
        'operand': {
            'labelId': label_id,
            'customerId': client_id
        }
    }
    return operation


def get_managed_customer_operation(fields=[], predicates=[], **kwargs):
    default_fields = kwargs.pop('default_fields', False)
    if default_fields:
        fields = set(fields).union({'CustomerId', 'Name'})
    return _get_selector(fields, predicates)
