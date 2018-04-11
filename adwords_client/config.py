import os
import yaml

FIELDS = {
    'developer_token': None,
    'client_customer_id': None,
    'client_id': None,
    'client_secret': None,
    'refresh_token': None,
}


def is_set():
    return all(FIELDS.values())


def configure_from_files(path):
    if not path:
        path = os.path.join(os.getcwd(), 'googleads.yaml')
        if not os.path.isfile(path):
            path = os.path.join(os.path.expanduser('~'), 'googleads.yaml')
            if not os.path.isfile(path):
                raise ValueError('Could not load settings for AdWords.')

    with open(path, 'r') as f:
        data = yaml.safe_load(f) or {}
    for key, value in data.get('adwords', {}).items():
        FIELDS[key] = value
    if not is_set():
        raise ValueError('Settings file is missing some configuration entries.')


def configure(path=None, **kwargs):
    if not is_set():
        for key, value in kwargs:
            FIELDS[key] = value
        if not is_set():
            for key in FIELDS:
                FIELDS[key] = os.environ.get(key.upper())
            if not is_set():
                configure_from_files(path)
