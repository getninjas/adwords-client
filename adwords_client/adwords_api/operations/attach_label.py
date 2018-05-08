

def attach_label_operation(label_id: 'Long' = None,
                 campaign_id: 'Long' = None,
                 ad_id: 'Long' = None,
                 ad_group_id: 'Long' = None,
                 criterion_id: 'Long' = None,
                 customer_id: 'Long' = None,
                 operator: 'String' = 'ADD',
                 **kwargs):

    operation = {
        'operator': operator.upper(),
        'operand': {
            'labelId': label_id,
        }
    }

    if customer_id:
        operation['operand']['customerId'] = customer_id
    if campaign_id:
        operation['operand']['campaignId'] = campaign_id
        operation['xsi_type'] = 'CampaignLabelOperation'
    if ad_group_id:
        operation['operand']['adGroupId'] = ad_group_id
        operation['xsi_type'] = 'AdGroupLabelOperation'
    if ad_id:
        operation['operand']['adId'] = ad_id
        operation['xsi_type'] = 'AdGroupAdLabelOperation'
    if criterion_id:
        operation['operand']['criterionId'] = criterion_id
        operation['xsi_type'] = 'AdGroupCriterionLabelOperation'

    return operation
