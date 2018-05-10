def _build_new_bid_type(xsi_type, value):
    bid_type = {
        'xsi_type': xsi_type,
        'bid': {
            'xsi_type': 'Money',
            'microAmount': value
        }
    }
    return bid_type


def _build_new_bidding_strategy_configuration(with_bids=True, strategy_type=None):
    bidding_strategy = {'xsi_type': 'BiddingStrategyConfiguration'}
    if with_bids:
        bidding_strategy['bids'] = []
    if strategy_type:
        bidding_strategy['biddingStrategyType'] = strategy_type
    return bidding_strategy


def _build_money(money):
    return {
        'xsi_type': 'Money',
        'microAmount': money,
    }


def batch_job_operation(operator, id_=None, status=None):
    operation = {
        'xsi_type': 'BatchJobOperation',
        'operand': {
            'xsi_type': 'BatchJob',
        },
        'operator': operator,
    }
    if id_:
        operation['operand']['id'] = id_
    if status:
        operation['operand']['status'] = status
    return operation


def get_batch_job_operation(fields=[], predicates=[], **kwargs):
    default_fields = kwargs.pop('default_fields', False)
    if default_fields:
        fields = set(fields).union({'Id', 'DownloadUrl'})
    return _get_selector(fields, predicates)


def _get_selector(fields, predicates=None, ordering=None):

    selector = {
        'xsi_type': 'Selector',
        'paging': {
            'xsi_type': 'Paging',
            'startIndex': 0,
            # maximum number of results allowed by API
            # https://developers.google.com/adwords/api/docs/appendix/limits#general
            'numberResults': 10000,
        },
        'fields': [],
        'predicates': [],
        'ordering': [],
    }

    selector['fields'].extend(fields)

    for field, operator, values in predicates:
        if isinstance(values, str) or isinstance(values, int) or isinstance(values, float):
            values = [values]
        predicate = {
            'xsi_type': 'Predicate',
            'field': field,
            'operator': operator,
            'values': values,
        }
        selector['predicates'].append(predicate)

    return selector
