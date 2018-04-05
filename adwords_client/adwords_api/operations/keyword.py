from .utils import _build_new_bid_type, _build_new_bidding_strategy_configuration


def new_keyword_operation(adgroup_id: 'Long' = None,
                          criteria_id: 'Long' = None,
                          text: 'String' = None,
                          keyword_match_type: 'String' = None,
                          status: 'String' = None,
                          cpc_bid: 'Bid' = None,
                          operator: 'String' = 'ADD',
                          **kwargs):
    operator = operator.upper()
    if not status and operator == 'ADD':
        status = 'PAUSED'

    operation = {
        'xsi_type': 'AdGroupCriterionOperation',
        'operand': {
            'xsi_type': 'BiddableAdGroupCriterion',
            'criterion': {'xsi_type': 'Keyword'},
            'adGroupId': adgroup_id,
        },
        'operator': operator
    }

    if criteria_id:
        operation['operand']['criterion']['id'] = criteria_id

    if status:
        operation['operand']['userStatus'] = status.upper()

    if status != 'REMOVED' and keyword_match_type and text:
        operation['operand']['criterion']['text'] = text
        operation['operand']['criterion']['matchType'] = keyword_match_type.upper()

    if status != 'REMOVED' and cpc_bid:
        bidding_strategy = _build_new_bidding_strategy_configuration()
        bid_type = _build_new_bid_type('CpcBid', cpc_bid)
        operation['operand']['biddingStrategyConfiguration'] = bidding_strategy
        operation['operand']['biddingStrategyConfiguration']['bids'].append(bid_type)

    return operation
