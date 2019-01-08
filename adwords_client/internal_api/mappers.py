import re
import math
from ..adwords_api import operations
from datetime import datetime
import dateutil.parser

double_regex = re.compile(r'[^\d.]+')


def process_double(x):
    """
    Transform a string having a Double to a python Float

    >>> process_double('123.456')
    123.456
    """
    x = double_regex.sub('', x)
    return float(x) if x else 0.0


integer_regex = re.compile(r'[^\d]+')


def process_integer(x):
    x = integer_regex.sub('', x)
    return int(x) if x else 0


def _float_as_cents(x):
    return max(0.01, float(math.ceil(100.0 * x)) / 100.0)


def cents_as_money(x):
    if math.isfinite(x):
        return int(round(_float_as_cents(x) * 1000000, 0))
    return None


def raw_money_as_cents(x):
    return process_double(x) / 1000000


def noop(x):
    return x


def cast_float(x):
    return float(x)


def cast_int(x):
    return int(x)


def process_list_of_str(x):
    return [str(item) for item in x]


def process_str_to_datetime(x):
    return datetime.strptime(x, '%Y%m%d %H%M%S')


def process_json_datetime_to_adw_format(x):
    datetime_converted = dateutil.parser.parse(x)
    return format(datetime_converted, '%Y%m%d %H%M%S')


class AdwordsMapper:
    def __init__(self, converter=noop, adapter=noop):
        self.converter = converter
        self.adapter = adapter

    def to_adwords(self, value):
        try:
            return self.adapter(value)
        except ValueError:
            return None

    def from_adwords(self, value):
        try:
            return self.converter(value)
        except ValueError:
            return None

    @property
    def from_adwords_func(self):
        return self.converter


MAPPERS = {
    'Money': AdwordsMapper(raw_money_as_cents, cents_as_money),
    'Bid': AdwordsMapper(raw_money_as_cents, cents_as_money),
    'Long': AdwordsMapper(process_integer, cast_int),
    'Double': AdwordsMapper(process_double, cast_float),
    'Integer': AdwordsMapper(process_integer, cast_int),
    'String': AdwordsMapper(str, str),
    'Identity': AdwordsMapper(noop, noop),
    'StringList': AdwordsMapper(process_list_of_str, process_list_of_str),
    'DateTime': AdwordsMapper(process_str_to_datetime, process_json_datetime_to_adw_format),
}

FIELD_MAP = {
    'client_id': 'Long',
    'campaign_id': 'Long',
    'budget_id': 'Long',
    'language_id': 'Long',
    'location_id': 'Long',
    'conversion_time': 'DateTime',
    'adjustment_time': 'DateTime',
    'start_date_time': 'DateTime',
    'end_date_time': 'DateTime',
}

FIELD_MAP.update(operations.keyword.new_keyword_operation.__annotations__)
FIELD_MAP.update(operations.adgroup.adgroup_operation.__annotations__)
FIELD_MAP.update(operations.ad.expanded_ad_operation.__annotations__)
FIELD_MAP.update(operations.campaign.add_budget.__annotations__)
FIELD_MAP.update(operations.campaign.campaign_operation.__annotations__)


def cast_to_adwords(field_name, field_value):
    return MAPPERS[FIELD_MAP.get(field_name, 'Identity')].to_adwords(field_value)
