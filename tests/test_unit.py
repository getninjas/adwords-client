from adwords_client.client import AdWords
import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger('googleads').setLevel(logging.ERROR)
logging.getLogger('oauth2client').setLevel(logging.ERROR)
logging.getLogger('suds').setLevel(logging.WARNING)
logging.getLogger('adwords_client').setLevel(logging.DEBUG)


def _delete_campaigns():
    client = AdWords(workdir='./tests')
    client.get_campaigns_report(7857288943, 'campaigns_report', 'CampaignStatus != "REMOVED"', create_table=True)
    new_report_df = client.load_table('campaigns_report')
    for cmp in new_report_df.itertuples():
        entry = {
            'object_type': 'campaign',
            'client_id': 7857288943,
            'campaign_id': int(cmp.CampaignId),
            'campaign_name': 'API test campaign',
            'operator': 'SET',
            'status': 'REMOVED',
        }
        client.insert(entry)
    operations_folder = client.split()
    client.execute_operations(operations_folder)
    client.wait_jobs(operations_folder)


def _create_campaign():
    client = AdWords(workdir='./tests')
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
    client = client or AdWords(workdir='./tests')
    client.get_keywords_report(7857288943, 'keywords_report', 'CampaignStatus = "PAUSED"', fields=True)
    report_df = client.load_table('keywords_report')
    print(report_df[['AccountDescriptiveName', 'AdGroupName', 'CampaignName', 'Criteria', 'KeywordMatchType', 'CpcBid']].to_string(index=False))
    return report_df


def _adjust_bids():
    client = AdWords(workdir='./tests')
    report_df = _get_keywords_report(client)

    for cmp in report_df.itertuples():
        entry = {
            'object_type': 'keyword',
            'cpc_bid': 4.20,
            'client_id': int(cmp.ExternalCustomerId),
            'campaign_id': int(cmp.CampaignId),
            'adgroup_id': int(cmp.AdGroupId),
            'criteria_id': int(cmp.Id),
            'operator': 'SET',
        }
        client.insert(entry)

    operations_folder = client.split()
    client.execute_operations(operations_folder)
    client.wait_jobs(operations_folder)


def test_client():
    _delete_campaigns()
    _create_campaign()
    _adjust_bids()
    _get_keywords_report()
    _delete_campaigns()
