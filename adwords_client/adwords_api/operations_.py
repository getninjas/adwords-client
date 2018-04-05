import logging

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
