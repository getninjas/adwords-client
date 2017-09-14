from adwords_client.client import AdWords
import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger('googleads').setLevel(logging.ERROR)
logging.getLogger('oauth2client').setLevel(logging.ERROR)


def test_pipeline():
    client = AdWords.autoload()
    client.wait_jobs()

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
