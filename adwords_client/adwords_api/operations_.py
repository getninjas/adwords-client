import logging

from adwords_client.adwords_api.operations.adgroup import adgroup_operation
from adwords_client.adwords_api.operations.campaign import add_budget
from adwords_client.adwords_api.operations.utils import _build_new_bid_type, _build_new_bidding_strategy_configuration

logger = logging.getLogger(__name__)

# TODO: REMOVE ALL OPERATIONS FROM THIS FILE, IT IS DEPRECATED


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
        logger.debug("Create a budget using 'amount' %s", amount)
        if not id_builder:
            raise RuntimeError("'id_builder' callable should be provided for budgets to be created")
        budget_id = id_builder()
        yield add_budget(amount, budget_id)

    logger.debug("Apply budget '%s' to campaign '%s'", budget_id, campaign_id)
    yield set_campaign_budget(budget_id, campaign_id)


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


def add_keyword_cpc_bid_adjustment_operation(adgroup_id,
                                             criteria_id,
                                             value):
    bid_operation = new_biddable_adgroup_criterion_operation(
        adgroup_id,
        'SET',
        'Keyword',
        criteria_id
    )
    bidding_strategy = _build_new_bidding_strategy_configuration()
    bid_operation['operand']['biddingStrategyConfiguration'] = bidding_strategy
    bid_type = _build_new_bid_type('CpcBid', value)
    bid_operation['operand']['biddingStrategyConfiguration']['bids'].append(bid_type)
    return bid_operation


def add_adgroup_cpc_bid_adjustment_operation(campaign_id,
                                             adgroup_id,
                                             value):
    bid_operation = adgroup_operation(campaign_id,
                                      adgroup_id,
                                      operator='SET',
                                      status=None)
    bidding_strategy = _build_new_bidding_strategy_configuration()
    bidding_strategy['bids'].append(_build_new_bid_type('CpcBid', value))
    bid_operation['operand']['biddingStrategyConfiguration'] = bidding_strategy
    return bid_operation
