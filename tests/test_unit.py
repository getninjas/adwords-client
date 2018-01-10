from adwords_client.client import AdWords
from adwords_client.client_operations import ClientOperation
from adwords_client.mappers.sqlite import SqliteMapper
from adwords_client.sqlite import get_connection
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
logging.getLogger('googleads').setLevel(logging.ERROR)
logging.getLogger('oauth2client').setLevel(logging.ERROR)
logging.getLogger('suds').setLevel(logging.WARNING)


def get_conn():
    return get_connection('test_db.sqlite')


def test_client_operations():
    objects = [
        {
            'object_type': 'campaign',
            'client_id': 7857288943,
            'campaign_id': -1,
            'budget': 1000,
            'campaign_name': 'API test campaign'
        },
        {
            'object_type': 'adgroup',
            'client_id': 7857288943,
            'campaign_id': -1,
            'adgroup_id': -2,
            'adgroup_name': 'API test adgroup',
        },
        {
            'object_type': 'keyword',
            'client_id': 7857288943,
            'campaign_id': -1,
            'adgroup_id': -2,
            'text': 'my search term',
            'keyword_match_type': 'broad',
            'status': 'paused',
            'cpc_bid': 13.37,
        },
    ]
    df = pd.DataFrame.from_dict(objects)
    df.to_sql('create_table', get_conn(), index=False, if_exists='replace')

    operation_fields = {
        'adgroup_id': 'adgroup_id',
        'adgroup_name': 'adgroup_name',
        'budget': 'budget',
        'campaign_id': 'campaign_id',
        'campaign_name': 'campaign_name',
        'client_id': 'client_id',
        'cpc_bid': 'cpc_bid',
        'keyword_match_type': 'keyword_match_type',
        'object_type': 'object_type',
        'status': 'status',
        'text': 'text',
    }

    mapper = SqliteMapper(operation_fields, get_conn)
    op = ClientOperation(mapper)

    op.run(AdWords.sync_objects, 'create_table', batchlog_table='batchlog_table')

    mapper = SqliteMapper({}, get_conn)
    op = ClientOperation(mapper)

    op.run(AdWords.wait_jobs, 'batchlog_table', batchlog_table='batchlog_table',
           drop_batchlog_table=True, n_procs=1)


def test_client():
    client = AdWords.autoload()

    client.get_campaigns_report(7857288943, 'campaigns_report', 'CampaignStatus != "REMOVED"', create_table=True)
    new_report_df = client.load_table('campaigns_report')
    client.clear('delete_operations')
    for cmp in new_report_df.itertuples():
        entry = {
            'object_type': 'campaign',
            'client_id': 7857288943,
            'campaign_id': int(cmp.CampaignId),
            'campaign_name': 'API test campaign',
            'operator': 'SET',
            'status': 'REMOVED',
        }
        client.insert('delete_operations', entry)
    client.sync_objects('delete_operations')
    client.wait_jobs()

    client.insert(
        'new_objects_table',
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
        'new_objects_table',
        {
            'object_type': 'adgroup',
            'client_id': 7857288943,
            'campaign_id': -1,
            'adgroup_id': -2,
            'adgroup_name': 'API test adgroup',
        }
    )
    client.insert(
        'new_objects_table',
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
        'new_objects_table',
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
    client.sync_objects('new_objects_table')
    client.wait_jobs()

    client.get_keywords_report(7857288943, 'keywords_report', 'CampaignStatus = "PAUSED"', fields=True)
    report_df = client.load_table('keywords_report')
    print(report_df[['AccountDescriptiveName', 'AdGroupName', 'CampaignName', 'Criteria', 'KeywordMatchType', 'CpcBid']].to_string(index=False))

    report_df['NewCpcBid'] = 4.20
    table_mappings = {
        'new_bid': 'NewCpcBid',
        'old_bid': 'CpcBid',
        'client_id': 'ExternalCustomerId',
        'campaign_id': 'CampaignId',
        'adgroup_id': 'AdGroupId',
        'keyword_id': 'Id',
    }
    client.dump_table(report_df, 'new_keywords_bid_table', table_mappings=table_mappings)
    client.modify_bids('new_keywords_bid_table')
    client.wait_jobs()

    client.get_keywords_report(7857288943, 'keywords_report', 'CampaignStatus = "PAUSED"', fields=True, create_table=True)
    new_report_df = client.load_table('keywords_report')
    print(new_report_df[['AccountDescriptiveName', 'AdGroupName', 'CampaignName', 'Criteria', 'KeywordMatchType', 'CpcBid']].to_string(index=False))

    client.clear('delete_operations')
    client.insert(
        'delete_operations',
        {
            'object_type': 'campaign',
            'client_id': 7857288943,
            'campaign_id': int(new_report_df['CampaignId'][0]),
            'campaign_name': 'API test campaign',
            'operator': 'SET',
            'status': 'REMOVED',
        }
    )
    client.sync_objects('delete_operations')
    client.wait_jobs()
