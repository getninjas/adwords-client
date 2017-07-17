import logging
import math
import uuid

logger = logging.getLogger(__name__)


def float_as_cents(x):
    return max(0.01, float(math.ceil(100.0 * x)) / 100.0)


def money_as_cents(x):
    return float(x) / 1000000


def cents_as_money(x):
    return int(round(float_as_cents(x) * 1000000, 0))


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


def add_ad(adgroup_id, ad_dict):
    operation = {
        'xsi_type': 'AdGroupAdOperation',
        'operand': {
            'xsi_type': 'AdGroupAd',
            'adGroupId': adgroup_id,
            'ad': ad_dict,
            'status': 'PAUSED',
            # TODO: 'labels': [],
        },
        'operator': 'ADD'
    }
    return operation


def build_ad(headline1, headline2, description, urls, ad_id=None, adtype='ExpandedTextAd'):
    result = {
        'xsi_type': adtype,
        'headlinePart1': headline1,
        'headlinePart2': headline2,
        'description': description,
        'finalUrls': list(urls),
    }
    if ad_id:
        result['id'] = ad_id
    return result


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


def add_campaign(campaign_id, campaign_name):
    operation = {
        'xsi_type': 'CampaignOperation',
        'operator': 'ADD',
        'operand': {
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


def add_budget(amount, budget_id, delivery='ACCELERATED', shared=False):
    operation = {
        'xsi_type': 'BudgetOperation',
        'operator': 'ADD',

        'operand': {
            'xsi_type': 'Budget',
            'budgetId': int(budget_id),
            'amount': build_money(float(amount)),
        },
    }

    if delivery:
        operation['operand']['deliveryMethod'] = delivery

    if shared:
        operation['operand'].update({
            'isExplicitlyShared': True,
            'name': 'automatic-%s' % uuid.uuid1()
        })
    else:
        operation['operand']['isExplicitlyShared'] = False

    return operation


def build_money(money):
    return {
        'xsi_type': 'Money',
        'microAmount': cents_as_money(money),
    }


def add_biddable_adgroup_criterion_operation(adgroup_id,
                                             operator,
                                             xsi_type,
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


def build_new_bidding_strategy_configuration():
    return {'xsi_type': 'BiddingStrategyConfiguration', 'bids': []}


def add_keyword_cpc_bid_adjustment_operation(adgroup_id,
                                             criteria_id,
                                             value):
    bid_operation = add_biddable_adgroup_criterion_operation(
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


def add_new_keyword_operation(adgroup_id,
                              text,
                              match_type,
                              status,
                              value):
    new_keyword_operation = add_biddable_adgroup_criterion_operation(
        adgroup_id,
        'ADD',
        'Keyword',
        criterion_params={
            'text': text,
            'matchType': match_type.upper(),
        },
        userStatus=status.upper()
    )
    bidding_strategy = build_new_bidding_strategy_configuration()
    new_keyword_operation['operand']['biddingStrategyConfiguration'] = bidding_strategy
    bid_type = build_new_bid_type('CpcBid', value)
    new_keyword_operation['operand']['biddingStrategyConfiguration']['bids'].append(bid_type)
    return new_keyword_operation


def add_adgroup(campaign_id, adgroup_id, operator='ADD'):
    operation = {
        'xsi_type': 'AdGroupOperation',
        'operand': {
            'xsi_type': 'AdGroup',
            'campaignId': campaign_id,
            'id': adgroup_id,
        },
        'operator': operator,
    }
    return operation


def add_adgroup_cpc_bid_adjustment_operation(campaign_id,
                                             adgroup_id,
                                             value):
    bid_operation = add_adgroup(campaign_id,
                                adgroup_id,
                                'SET')
    bidding_strategy = build_new_bidding_strategy_configuration()
    bid_operation['operand']['biddingStrategyConfiguration'] = bidding_strategy
    bid_type = build_new_bid_type('CpcBid', value)
    bid_operation['operand']['biddingStrategyConfiguration']['bids'].append(bid_type)
    return bid_operation
