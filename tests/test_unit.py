import logging
from pprint import pprint

from adwords_client.client import AdWords
from adwords_client import reports

logging.basicConfig(level=logging.INFO)
logging.getLogger('googleads').setLevel(logging.ERROR)
logging.getLogger('oauth2client').setLevel(logging.ERROR)
logging.getLogger('suds').setLevel(logging.WARNING)


def _delete_campaigns():
    client = AdWords(workdir='./tests/generated_files')
    new_report_df = reports.get_campaigns_report(client, 7857288943, 'CampaignStatus != "REMOVED"')
    for campaign in new_report_df:
        entry = {
            'object_type': 'campaign',
            'client_id': 7857288943,
            'campaign_id': campaign['CampaignId'],
            'campaign_name': 'API test campaign',
            'operator': 'SET',
            'status': 'REMOVED',
        }
        client.insert(entry)
    operations_folder = client.split()
    client.execute_operations(operations_folder)
    client.wait_jobs(operations_folder)


def _create_campaign():
    client = AdWords(workdir='./tests/generated_files')
    client.insert(
        {
            'object_type': 'campaign',
            'client_id': 7857288943,
            'campaign_id': -1,
            'budget': 1000,
            'campaign_name': 'API test campaign',
            'locations': [1001773, 1001768],  # Sao Paulo, Sao Caetano
            'languages': [1014, 1000],  # Portuguese, English
        }
    )
    client.insert(
        {
            'object_type': 'adgroup',
            'client_id': 7857288943,
            'campaign_id': -1,
            'adgroup_id': -2,
            'adgroup_name': 'API test adgroup',
        }
    )
    client.insert(
        {
            'object_type': 'keyword',
            'client_id': 7857288943,
            'campaign_id': -1,
            'adgroup_id': -2,
            'text': 'my search term',
            'keyword_match_type': 'broad',
            'status': 'paused',
            'cpc_bid': 13.37,
        }
    )
    client.insert(
        {
            'object_type': 'ad',
            'client_id': 7857288943,
            'campaign_id': -1,
            'adgroup_id': -2,
            'headline_part_1': 'Ad test',
            'headline_part_2': 'my pretty test',
            'description': 'This is my test ad',
            'path_1': 'test',
            'path_2': 'ad',
            'final_urls': 'http://www.mytest.com/',
            'final_mobile_urls': 'http://m.mytest.com/',
        }
    )
    operations_folder = client.split()
    client.execute_operations(operations_folder)
    client.wait_jobs(operations_folder)


def _get_keywords_report(client=None):
    client = client or AdWords(workdir='./tests/generated_files')
    report_df = reports.get_keywords_report(client, 7857288943, 'CampaignStatus = "PAUSED"', fields=True)
    pprint(report_df)
    return report_df


def _adjust_bids():
    client = AdWords(workdir='./tests/generated_files')
    report_df = _get_keywords_report(client)

    for campaign in report_df:
        entry = {
            'object_type': 'keyword',
            'cpc_bid': 4.20,
            'client_id': campaign['ExternalCustomerId'],
            'campaign_id': campaign['CampaignId'],
            'adgroup_id': campaign['AdGroupId'],
            'criteria_id': campaign['Id'],
            'operator': 'SET',
        }
        client.insert(entry)

    operations_folder = client.split()
    client.execute_operations(operations_folder)
    client.wait_jobs(operations_folder)


def _sync_operations():
    client = AdWords()

    client.insert({
        'object_type': 'shared_set',
        'client_id': 7857288943,
        'shared_set_name': 'API Sync Test',
        'shared_set_type': 'NEGATIVE_KEYWORDS',
    })

    client.execute_operations(sync=True)

    get_internal_operation = {
        'object_type': 'shared_set',
        'client_id': 7857288943,
        'fields': ['Name', 'SharedSetId', 'Type', 'Status'],
        'predicates': [{
            'field': 'Name',
            'operator': 'EQUALS',
            'values': ['API Sync Test']
        }, {
            'field': 'Status',
            'operator': 'EQUALS',
            'values': ['ENABLED']
        },
        ]
    }

    shared_sets = client.get_entities(get_internal_operation)

    for shared_set in shared_sets:
        client.insert({
            'object_type': 'shared_set',
            'client_id': 7857288943,
            'shared_set_id': shared_set['sharedSetId'],
            'operator': 'REMOVE'
        })

    client.execute_operations(sync=True)


def test_client():
    _delete_campaigns()
    _create_campaign()
    _adjust_bids()
    _get_keywords_report()
    _delete_campaigns()
    _sync_operations()


def test_get_accounts():
    client = AdWords(workdir='./tests/generated_files')
    client.get_accounts()
