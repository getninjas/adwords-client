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
    return max(0.01, float(math.ceil(100.0*x))/100.0)


def money_as_cents(x):
    return float(x) / 1000000


def cents_as_money(x):
    return int(round(float_as_cents(x) * 1000000, 0))
