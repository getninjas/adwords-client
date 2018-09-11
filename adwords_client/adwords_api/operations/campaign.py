from .utils import _build_new_bidding_strategy_configuration, _build_money, _get_selector


def campaign_operation(campaign_id: 'Long' = None,
                       campaign_name: 'String' = None,
                       budget_id: 'Long' = None,
                       status: 'String' = None,
                       advertising_channel: 'String' = 'SEARCH',
                       network_setting: 'StringList' = None,
                       ad_serving_optimization_status: 'String' = None,
                       positive_geo_target_type: 'String' = None,
                       operator: 'String' = 'ADD',
                       **kwargs):
    bidding_strategy = _build_new_bidding_strategy_configuration(with_bids=False, strategy_type='MANUAL_CPC')
    operation = {
        'xsi_type': 'CampaignOperation',
        'operator': operator.upper(),
        'operand': {
            # https://developers.google.com/adwords/api/docs/reference/v201705/CampaignService.Campaign
            'xsi_type': 'Campaign',
            'id': campaign_id,
        },
    }
    if status:
        operation['operand']['status'] = status
    if campaign_name:
        operation['operand']['name'] = campaign_name
    # Note that only the budgetId is required
    if budget_id:
        operation['operand']['budget'] = {'budgetId': budget_id}
    if bidding_strategy:
        operation['operand']['biddingStrategyConfiguration'] = bidding_strategy
    if advertising_channel:
        operation['operand']['advertisingChannelType'] = advertising_channel
    if ad_serving_optimization_status:
        operation['operand']['adServingOptimizationStatus'] = ad_serving_optimization_status
    if network_setting:
        operation['operand']['networkSetting'] = {
                'targetGoogleSearch': False,
                'targetSearchNetwork': False,
                'targetContentNetwork': False,
                'targetPartnerSearchNetwork': False
            }
        for network in network_setting:
            operation['operand']['networkSetting'][network] = True
    if positive_geo_target_type:
        operation['operand']['settings'] = {
                'xsi_type': 'GeoTargetTypeSetting',
                'positiveGeoTargetType': positive_geo_target_type,
            }
    return operation


def add_campaign_language(campaign_id: 'Long' = None,
                          language_id: 'Long' = None,
                          operator: 'String' = 'ADD',
                          **kwargs):
    operation = {
        # https://developers.google.com/adwords/api/docs/reference/v201708/CampaignCriterionService.CampaignCriterionOperation
        'xsi_type': 'CampaignCriterionOperation',
        'operator': operator.upper(),
        'operand': {
            # https://developers.google.com/adwords/api/docs/reference/v201708/CampaignCriterionService.CampaignCriterion
            'xsi_type': 'CampaignCriterion',
            'campaignId': campaign_id,
            'criterion': {
                # https://developers.google.com/adwords/api/docs/reference/v201708/CampaignCriterionService.Language
                'xsi_type': 'Language',
                'id': language_id,
            }
        },
    }
    return operation


def add_campaign_location(campaign_id: 'Long' = None,
                          location_id: 'Long' = None,
                          operator: 'String' = 'ADD',
                          bid_modifier: 'Double' = None,
                          **kwargs):
    operation = {
        # https://developers.google.com/adwords/api/docs/reference/v201708/CampaignCriterionService.CampaignCriterionOperation
        'xsi_type': 'CampaignCriterionOperation',
        'operator': operator.upper(),
        'operand': {
            # https://developers.google.com/adwords/api/docs/reference/v201708/CampaignCriterionService.CampaignCriterion
            'xsi_type': 'CampaignCriterion',
            'campaignId': campaign_id,
            'criterion': {
                # https://developers.google.com/adwords/api/docs/reference/v201708/CampaignCriterionService.Location
                'xsi_type': 'Location',
                'id': location_id,
            }
        },
    }
    return operation


def add_budget(budget: 'Money' = None,
               budget_id: 'Long' = None,
               delivery: 'String' = 'ACCELERATED',
               budget_name: 'String' = None,
               **kwargs):
    operation = {
        'xsi_type': 'BudgetOperation',
        'operator': 'ADD',

        'operand': {
            'xsi_type': 'Budget',
            'budgetId': int(budget_id),
            'amount': _build_money(budget),
        },
    }

    if delivery:
        operation['operand']['deliveryMethod'] = delivery

    if budget_name:
        operation['operand'].update({
            'isExplicitlyShared': True,
            'name': budget_name
        })
    else:
        operation['operand']['isExplicitlyShared'] = False

    return operation


def get_campaign_operation(fields=[], predicates=[], **kwargs):
    default_fields = kwargs.pop('default_fields', False)
    if default_fields:
        fields = set(fields).union({'Id', 'Name'})
    return _get_selector(fields, predicates)
