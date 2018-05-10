import logging
from .utils import _get_selector

logger = logging.getLogger(__name__)


def new_label_operation(label: 'String'= None,
                        operator: 'String' = 'ADD',
                        **kwargs):
    if not label:
        raise ValueError('Argument "label" must be provided in operation.')
    operator = operator.upper()
    return {
        'xsi_type': 'LabelOperation',
        'operator': operator,
        'operand': {
            'xsi_type': 'TextLabel',
            'name': label
        }
    }


def get_label_operation(fields=[], predicates=[],  **kwargs):
    default_fields = kwargs.pop('default_fields', False)
    if default_fields:
        fields = set(fields).union({'LabelId', 'LabelName'})
    return _get_selector(fields, predicates)
