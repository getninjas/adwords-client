from .utils import _get_selector


def managed_customer_operation(client_id: 'Long' = None,
                               name: 'String' = None,
                               currency_code: 'String' = 'BRL',
                               operator: 'String' = 'ADD',
                               date_time_zone: 'String' = 'America/Sao_Paulo',
                               **kwargs):
    operation = {
        'operator': operator.upper(),
        'operand': {
            'currencyCode': currency_code,
            'dateTimeZone': date_time_zone,
        }
    }
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
    fields = set(fields).union({'CustomerId', 'Name'})
    return _get_selector(fields, predicates)
