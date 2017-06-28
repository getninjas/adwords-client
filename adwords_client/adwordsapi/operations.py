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


def add_adgroup(self, campaign_id, adgroup_id, operator):
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


def add_adgroup_operation(campaign_id, adgroup_id, operator):
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
    bid_operation = add_adgroup_operation(campaign_id,
                                          adgroup_id,
                                          'SET')
    bidding_strategy = build_new_bidding_strategy_configuration()
    bid_operation['operand']['biddingStrategyConfiguration'] = bidding_strategy
    bid_type = build_new_bid_type('CpcBid', value)
    bid_operation['operand']['biddingStrategyConfiguration']['bids'].append(bid_type)
    return bid_operation
