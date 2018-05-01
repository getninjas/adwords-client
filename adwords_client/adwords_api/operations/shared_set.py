

def shared_set_operation(shared_set_id: 'Long' = None,
                       shared_set_name: 'String' = None,
                       shared_set_type: 'String' = 'NEGATIVE_KEYWORDS',
                       operator: 'String' = 'ADD',
                       **kwargs):
    operation = {
        'operator': operator.upper(),
        'operand': {
            'name': shared_set_name,
            'type': shared_set_type,
        }
    }
    if shared_set_id:
        operation['operand']['id'] = shared_set_id
    return operation
