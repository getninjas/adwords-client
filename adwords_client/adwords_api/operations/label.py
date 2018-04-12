import logging

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
