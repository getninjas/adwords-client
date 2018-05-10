from .utils import _build_new_bidding_strategy_configuration, _build_money, _get_selector


def campaign_operation(campaign_id: 'Long' = None,
                       campaign_name: 'String' = None,
                       budget_id: 'Long' = None,
                       status: 'String' = 'PAUSED',
                       advertising_channel: 'String' = 'SEARCH',
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

            ## From: https://developers.google.com/adwords/api/docs/samples/python/campaign-management#add-complete-campaigns-using-batch-jobs
            # 'advertisingChannelType': 'SEARCH',
            # Recommendation: Set the campaign to PAUSED when creating it to
            # stop the ads from immediately serving. Set to ENABLED once
            # you've added targeting and the ads are ready to serve.
            'status': status,
        },
    }
    if campaign_name:
        operation['operand']['name'] = campaign_name
    # Note that only the budgetId is required
    if budget_id:
        operation['operand']['budget'] = {'budgetId': budget_id}
    if bidding_strategy:
        operation['operand']['biddingStrategyConfiguration'] = bidding_strategy
    if advertising_channel:
        operation['operand']['advertisingChannelType'] = advertising_channel
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
