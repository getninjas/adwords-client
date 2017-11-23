import logging

logger = logging.getLogger(__name__)


def add_label_operation(label):
    yield {
        'xsi_type': 'LabelOperation',
        'operator': 'ADD',
        'operand': {
            'xsi_type': 'TextLabel',
            'name': label
        }
    }


def add_adgroup_label_operation(adgroup_id, label_id):
    yield {
        'xsi_type': 'AdGroupLabelOperation',
        'operator': 'ADD',
        'operand': {
            'xsi_type': 'AdGroupLabel',
            'adGroupId': adgroup_id,
            'labelId': label_id
        }
    }


def set_adgroup_name_operation(adgroup_id, name):
    yield {
        'xsi_type': 'AdGroupOperation',
        'operator': 'SET',
        'operand': {
            'xsi_type': 'AdGroup',
            'id': adgroup_id,
            'name': name
        }
    }


def apply_new_budget(campaign_id, amount=None, budget_id=None, id_builder=None):
    if not budget_id:
        logger.debug("Create a budget using 'amount' %s" % amount)
        if not id_builder:
            raise RuntimeError("'id_builder' callable should be provided for budgets to be created")
        budget_id = id_builder()
        yield add_budget(amount, budget_id)

    logger.debug("Apply budget '%s' to campaign '%s'" % (budget_id, campaign_id))
    yield set_campaign_budget(budget_id, campaign_id)


def add_restriction(adgroup_id, restriction_dict, effect='SHOW'):
    """
    A restriction to display an Ad. For example, a Keyword dict.
    If 'effect' is "HIDE", will enforce the Ad to NOT be displayed
    when the restriction got matched.
    """
    if effect.upper() == 'SHOW':
        operand_type = 'BiddableAdGroupCriterion'
    elif effect.upper() == 'HIDE':
        operand_type = 'NegativeAdGroupCriterion'
    else:
        raise NotImplementedError("Desired 'effect' was not recognized")

    operation = {
        'xsi_type': 'AdGroupCriterionOperation',
        'operand': {
            'xsi_type': operand_type,
            'adGroupId': adgroup_id,
            'criterion': restriction_dict,
        },
        'operator': 'ADD'
    }
    return operation


def build_keyword(text, keyword_id=None, match='BROAD'):
    result = {
        'xsi_type': 'Keyword',
        'text': text,
        'matchType': 'BROAD',  # Only EXACT, PHRASE or BROAD
    }
    if keyword_id:
        result['id'] = keyword_id
    return result


def campaign_operation(campaign_id: 'Long' = None,
                       campaign_name: 'String' = None,
                       budget_id: 'Long' = None,
                       status: 'String' = 'PAUSED',
                       advertising_channel: 'String' = 'SEARCH',
                       operator: 'String' = 'ADD',
                       **kwargs):
    bidding_strategy = build_new_bidding_strategy_configuration(with_bids=False, strategy_type='MANUAL_CPC')
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


def add_account(campaign_id, campaign_name):
    operation = {
        'xsi_type': 'CampaignOperation',
        'operator': 'ADD',
        'operand': {
            # https://developers.google.com/adwords/api/docs/reference/v201705/CampaignService.Campaign
            'xsi_type': 'Campaign',
            'id': campaign_id,
            'name': campaign_name,

            ## From: https://developers.google.com/adwords/api/docs/samples/python/campaign-management#add-complete-campaigns-using-batch-jobs
            # 'advertisingChannelType': 'SEARCH',
            # Recommendation: Set the campaign to PAUSED when creating it to
            # stop the ads from immediately serving. Set to ENABLED once
            # you've added targeting and the ads are ready to serve.
            'status': 'PAUSED',
            # Note that only the budgetId is required
            # 'budget': {
            #     'budgetId': budget_id
            # },
            # 'biddingStrategyConfiguration': {
            #     'biddingStrategyType': 'MANUAL_CPC'
            # }
        },
    }
    return operation


def set_campaign_budget(budget_id, campaign_id):
    return {
        'xsi_type': 'CampaignOperation',
        'operator': 'SET',

        'operand': {
            'id': int(campaign_id),
            'budget': {
                'xsi_type': 'Budget',
                'budgetId': int(budget_id),
            },
        },
    }


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
            'amount': build_money(budget),
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


def build_money(money):
    return {
        'xsi_type': 'Money',
        'microAmount': money,
    }


def new_biddable_adgroup_criterion_operation(adgroup_id=None,
                                             operator=None,
                                             xsi_type=None,
                                             criteria_id=None,
                                             criterion_params={},
                                             **kwargs):
    criterion = {'xsi_type': xsi_type}
    if criteria_id:
        criterion['id'] = criteria_id
    for key in criterion_params:
        criterion[key] = criterion_params[key]

    operand = {
        'xsi_type': 'BiddableAdGroupCriterion',
        'criterion': criterion,
        'adGroupId': adgroup_id,
    }
    for key in kwargs:
        operand[key] = kwargs[key]

    operation = {
        'xsi_type': 'AdGroupCriterionOperation',
        'operand': operand,
        'operator': operator
    }
    return operation


def build_new_bid_type(xsi_type, value):
    bid_type = {
        'xsi_type': xsi_type,
        'bid': {
            'xsi_type': 'Money',
            'microAmount': value
        }
    }
    return bid_type


def build_new_bidding_strategy_configuration(with_bids=True, strategy_type=None):
    bidding_strategy = {'xsi_type': 'BiddingStrategyConfiguration'}
    if with_bids:
        bidding_strategy['bids'] = []
    if strategy_type:
        bidding_strategy['biddingStrategyType'] = strategy_type
    return bidding_strategy


def add_keyword_cpc_bid_adjustment_operation(adgroup_id,
                                             criteria_id,
                                             value):
    bid_operation = new_biddable_adgroup_criterion_operation(
        adgroup_id,
        'SET',
        'Keyword',
        criteria_id
    )
    bidding_strategy = build_new_bidding_strategy_configuration()
    bid_operation['operand']['biddingStrategyConfiguration'] = bidding_strategy
    bid_type = build_new_bid_type('CpcBid', value)
    bid_operation['operand']['biddingStrategyConfiguration']['bids'].append(bid_type)
    return bid_operation


def new_keyword_operation(adgroup_id: 'Long' = None,
                          criteria_id: 'Long' = None,
                          text: 'String' = None,
                          keyword_match_type: 'String' = None,
                          status: 'String' = None,
                          cpc_bid: 'Bid' = None,
                          operator: 'String' = 'ADD',
                          **kwargs):
    status = status.upper()
    if not status and operator == 'ADD':
        status = 'PAUSED'
    operator = operator.upper()

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
        operation['operand']['userStatus'] = status

    if status != 'REMOVED' and keyword_match_type and text:
        operation['operand']['criterion']['text'] = text
        operation['operand']['criterion']['matchType'] = keyword_match_type.upper()

    if status != 'REMOVED' and cpc_bid:
        bidding_strategy = build_new_bidding_strategy_configuration()
        bid_type = build_new_bid_type('CpcBid', cpc_bid)
        operation['operand']['biddingStrategyConfiguration'] = bidding_strategy
        operation['operand']['biddingStrategyConfiguration']['bids'].append(bid_type)

    return operation

def expanded_text_ad(headline_part_1='',
                     headline_part_2='',
                     description='',
                     path_1='',
                     path_2='',
                     tracking_url_template=None,
                     url_customer_parameters=None,
                     final_urls=None,
                     final_mobile_urls=None,
                     final_app_urls=None):
    # https://developers.google.com/adwords/api/docs/reference/v201708/AdGroupAdService.Ad
    # https://developers.google.com/adwords/api/docs/reference/v201708/AdGroupAdService.ExpandedTextAd
    ad = {
        'xsi_type': 'ExpandedTextAd',

        # Expanded text ads fields
        'headlinePart1': headline_part_1,
        'headlinePart2': headline_part_2,
        'description': description,
        'path1': path_1,
        'path2': path_2,
    }
    # we assume at this point that only one final url will be set
    if final_urls:
        # Specify a list of final URLs. This field cannot be set if URL
        # field is set, or finalUrls is unset. This may be specified at ad,
        # criterion, and feed item levels.
        ad['finalUrls'] = [final_urls],
    # we assume at this point that only one final url will be set
    if final_mobile_urls:
        # Specify a list of final URLs. This field cannot be set if URL
        # field is set, or finalUrls is unset. This may be specified at ad,
        # criterion, and feed item levels.
        ad['finalMobileUrls'] = [final_mobile_urls],
        # we assume at this point that only one final url will be set
    if final_app_urls:
        # Specify a list of final URLs. This field cannot be set if URL
        # field is set, or finalUrls is unset. This may be specified at ad,
        # criterion, and feed item levels.
        ad['finalAppUrls'] = [final_app_urls],
    if final_urls:
        # Specify a list of final URLs. This field cannot be set if URL
        # field is set, or finalUrls is unset. This may be specified at ad,
        # criterion, and feed item levels.
        ad['finalUrls'] = [final_urls],
    if tracking_url_template:
        # Specify a tracking URL for 3rd party tracking provider. You may specify
        # one at customer, campaign, ad group, ad, criterion or feed item levels.
        ad['trackingUrlTemplate'] = tracking_url_template
    if url_customer_parameters:
        # Values for the parameters in the tracking URL. This can be provided at
        # campaign, ad group, ad, criterion, or feed item levels.
        parameters = []
        ad['urlCustomParameters'] = {'parameters': parameters}
        for k, v in url_customer_parameters.items():
            parameters.append({'key': k, 'value': v})
    return ad


def abstract_ad(ad_id: 'Long' = None):
    ad = {
        'xsi_type': 'Ad',
        # Abstract ad for set and remove operations
        'id': ad_id,
    }
    return ad


def expanded_ad_operation(adgroup_id: 'Long' = None,
                          ad_id: 'Long' = None,
                          status: 'String' = 'PAUSED',
                          operator: 'String' = 'ADD',
                          headline_part_1: 'String' = '',
                          headline_part_2: 'String' = '',
                          description: 'String' = '',
                          path_1: 'String' = '',
                          path_2: 'String' = '',
                          tracking_url_template: 'String' = None,
                          url_customer_parameters: 'String' = None,
                          final_urls: 'String' = None,
                          final_mobile_urls: 'String' = None,
                          final_app_urls: 'String' = None,
                          **kwargs):
    if operator.upper() == 'ADD':
        ad = expanded_text_ad(
                    headline_part_1,
                    headline_part_2,
                    description,
                    path_1,
                    path_2,
                    tracking_url_template,
                    url_customer_parameters,
                    final_urls,
                    final_mobile_urls,
                    final_app_urls,
                )
    else:
        ad = abstract_ad(ad_id)
    operation = {
        'xsi_type': 'AdGroupAdOperation',
        'operand': {
            # https://developers.google.com/adwords/api/docs/reference/v201708/AdGroupAdService.AdGroupAd
            'xsi_type': 'AdGroupAd',
            'adGroupId': adgroup_id,
            'ad': ad,
            'status': status,
        },
        'operator': operator,
    }
    return operation


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


def add_adgroup_cpc_bid_adjustment_operation(campaign_id,
                                             adgroup_id,
                                             value):
    bid_operation = adgroup_operation(campaign_id,
                                      adgroup_id,
                                      operator='SET',
                                      status=None)
    bidding_strategy = build_new_bidding_strategy_configuration()
    bidding_strategy['bids'].append(build_new_bid_type('CpcBid', value))
    bid_operation['operand']['biddingStrategyConfiguration'] = bidding_strategy
    return bid_operation
