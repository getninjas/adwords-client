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
        }
    }

    if shared_set_id:
        operation['operand']['sharedSetId'] = shared_set_id
    if shared_set_name:
        operation['operand']['name'] = shared_set_name
    if shared_set_type:
        operation['operand']['type'] = shared_set_type

    return operation


def get_shared_set_operation(fields=[], predicates=[], **kwargs):
    default_fields = kwargs.pop('default_fields', False)
    if default_fields:
        fields = set(fields).union({'SharedSetId', 'Name'})
    return _get_selector(fields, predicates)
