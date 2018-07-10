from .utils import _build_new_bid_type, _build_new_bidding_strategy_configuration, _get_selector


def adgroup_operation(campaign_id: 'Long' = None,
                      adgroup_id: 'Long' = None,
                      adgroup_name: 'String' = None,
                      status: 'String' = None,
                      cpc_bid: 'Bid' = None,
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

    if status != 'REMOVED' and cpc_bid:
        bidding_strategy = _build_new_bidding_strategy_configuration()
        bid_type = _build_new_bid_type('CpcBid', cpc_bid)
        operation['operand']['biddingStrategyConfiguration'] = bidding_strategy
        operation['operand']['biddingStrategyConfiguration']['bids'].append(bid_type)
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


def get_ad_group_operation(fields=[], predicates=[], **kwargs):
    default_fields = kwargs.pop('default_fields', False)
    if default_fields:
        fields = set(fields).union({'Id'})
    return _get_selector(fields, predicates)
