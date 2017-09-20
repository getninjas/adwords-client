import re
import math

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


def float_as_cents(x):
    return max(0.01, float(math.ceil(100.0 * x)) / 100.0)


def raw_money_as_cents(x):
    return process_double(x) / 1000000


def money_as_cents(x):
    return float(x) / 1000000


def cents_as_money(x):
    return int(round(float_as_cents(x) * 1000000, 0))


def noop(x):
    return x


class AdwordsMapper:
    def __init__(self, converter=noop, adapter=noop):
        self.converter = converter
        self.adapter = adapter

    def to_adwords(self, value):
        if value and (not isinstance(value, float) or (isinstance(value, float) and not math.isnan(value))):
            return self.adapter(value)
        else:
            return None

    def from_adwords(self, value):
        if value and (not isinstance(value, float) or (isinstance(value, float) and not math.isnan(value))):
            return self.converter(value)
        else:
            return None

    @property
    def from_adwords_func(self):
        return self.converter


MAPPERS = {
    'Money': AdwordsMapper(raw_money_as_cents, cents_as_money),
    'Bid': AdwordsMapper(raw_money_as_cents, cents_as_money),
    'Long': AdwordsMapper(process_integer, int),
    'Double': AdwordsMapper(process_double, float),
    'Integer': AdwordsMapper(process_integer, int),
    'String': AdwordsMapper(str, str),
    'Identity': AdwordsMapper(lambda x: x, lambda x: x),
}
