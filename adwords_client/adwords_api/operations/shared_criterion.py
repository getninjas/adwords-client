from .utils import _get_selector


def shared_criterion_operation(shared_criterion_id: 'Long' = None,
                               shared_set_id: 'Long' = None,
                               operator: 'String' = 'ADD',
                               is_negative: 'Bool' = True,
                               criterion_type: 'String' = 'Keyword',
                               match_type: 'String' = 'EXACT',
                               keyword_text: 'String' = None,
                               **kwargs):
    operation = {
        'xsi_type': 'SharedCriterionOperation',
        'operator': operator.upper(),
        'operand': {
            'xsi_type': 'SharedCriterion',
            'negative': is_negative,
            'criterion': {},
            'sharedSetId': shared_set_id
        }
    }

    if shared_criterion_id:
        operation['operand']['criterion']['id'] = shared_criterion_id
    if criterion_type == 'Keyword':
        operation['operand']['criterion'] = {
            'xsi_type': 'Keyword',
            'text': keyword_text,
            'matchType': match_type
        }
    return operation


def get_shared_criterion_operation(fields=[], predicates=[], **kwargs):
    default_fields = kwargs.pop('default_fields', False)
    if default_fields:
        fields = set(fields).union({'Id'})
    return _get_selector(fields, predicates)
