def adgroup_operation(campaign_id: 'Long' = None,
                      adgroup_id: 'Long' = None,
                      adgroup_name: 'String' = None,
                      status: 'String' = 'PAUSED',
                      operator: 'String' = 'ADD',
                      **kwargs):
    operation = {
        'xsi_type': 'AdGroupOperation',
        'operand': {
            # https://developers.google.com/adwords/api/docs/reference/v201705/AdGroupService.AdGroup
            'xsi_type': 'AdGroup',
            'campaignId': campaign_id,
            'id': adgroup_id,
        },
        'operator': operator,
    }
    if adgroup_name:
        operation['operand']['name'] = adgroup_name
    if status:
        operation['operand']['status'] = status
    return operation


def ad_group_label_operation(operator: 'String' = 'ADD',
                             ad_group_id: 'Long' = None,
                             label_id: 'Long' = None,
                             **kwargs):
    operation = {
        'operator': operator.upper(),
        'operand': {
            'adGroupId': ad_group_id,
            'labelId': label_id

        }
    }
    return operation
