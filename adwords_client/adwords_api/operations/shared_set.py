from .utils import _get_selector


def shared_set_operation(shared_set_id: 'Long' = None,
                       shared_set_name: 'String' = None,
                       shared_set_type: 'String' = 'NEGATIVE_KEYWORDS',
                       operator: 'String' = 'ADD',
                       **kwargs):
    operation = {
        'xsi_type': 'SharedSetOperation',
        'operator': operator.upper(),
        'operand': {
            'xsi_type': 'SharedSet',
            'name': shared_set_name,
            'type': shared_set_type,
        }
    }
    if shared_set_id:
        operation['operand']['id'] = shared_set_id
    return operation


def get_shared_set_operation(fields=[], predicates=[], **kwargs):
    fields = set(fields).union({'SharedSetId', 'Name'})
    return _get_selector(fields, predicates)
