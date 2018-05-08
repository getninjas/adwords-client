from .utils import _build_new_bidding_strategy_configuration, _build_money, _get_selector


def campaign_shared_set_operation(shared_set_id: 'Long' = None,
                         campaign_id: 'Long' = None,
                         operator: 'String' = 'ADD',
                         **kwargs):

    operation = {
        'xsi_type': 'CampaignSharedSetOperation',
        'operator': operator.upper(),
        'operand': {
            'xsi_type': 'CampaignSharedSet',
            'sharedSetId': shared_set_id,
            'campaignId': campaign_id,
        }
    }
    return operation


def get_campaign_shared_set(fields=[], predicates=[],  **kwargs):
    fields = set(fields).union({'SharedSetId', 'SharedSetName'})
    return _get_selector(fields, predicates)
