
#ltalvez colocar os atributos de keywords dentro de kwards?
def shared_criterion_operation(shared_criterion_id: 'Long' = None,
                               shared_set_id: 'Long' = None,
                               operator: 'String' = 'ADD',
                               is_negative: 'Bool' = True,
                               criterion_type: 'String' = 'Keyword',
                               match_type: 'String' = 'EXACT',
                               keyword_text: 'String' = None,
                               **kwargs):
    operation = {
        'operator': operator.upper(),
        'operand': {
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
