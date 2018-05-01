

def campaign_shared_set_operation(shared_set_id: 'Long' = None,
                         campaign_id: 'Long' = None,
                         operator: 'String' = 'ADD',
                         **kwargs):

    operation = {
        'operator': operator.upper(),
        'operand': {
            'sharedSetId': shared_set_id,
            'campaignId': campaign_id,
        }
    }
    return operation
