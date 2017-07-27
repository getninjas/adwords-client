===========
Basic Usage
===========

Client creation
---------------

    >>> from adwords_client.client import AdWords
    >>> client = AdWords.autoload()

.. setup:
    >>> import pandas as pd
    >>> client.get_campaigns_report(7857288943, 'setup_test_table', 'CampaignStatus != "REMOVED"', create_table=True)
    >>> new_report_df = client.load_table('setup_test_table')
    >>> objects = []
    >>> for cmp in new_report_df.itertuples():
    ...     entry = {
    ...         'object_type': 'campaign',
    ...         'client_id': 7857288943,
    ...         'campaign_id': cmp.CampaignId,
    ...         'campaign_name': 'API test campaign',
    ...         'operator': 'SET',
    ...         'status': 'REMOVED',
    ...     }
    ...     objects.append(entry)
    >>> if objects:
    ...     df = pd.DataFrame.from_dict(objects)
    ...     client.dump_table(df, 'setup_test_table')
    ...     client.sync_objects('setup_test_table')

Objects Creation
----------------

    >>> objects = [
    ...     {
    ...         'object_type': 'campaign',
    ...         'client_id': 7857288943,
    ...         'campaign_id': -1,
    ...         'budget': 1000,
    ...         'campaign_name': 'API test campaign'
    ...     },
    ...     {
    ...         'object_type': 'adgroup',
    ...         'client_id': 7857288943,
    ...         'campaign_id': -1,
    ...         'adgroup_id': -2,
    ...         'adgroup_name': 'API test adgroup',
    ...     },
    ...     {
    ...         'object_type': 'keyword',
    ...         'client_id': 7857288943,
    ...         'campaign_id': -1,
    ...         'adgroup_id': -2,
    ...         'text': 'my search term',
    ...         'keyword_match_type': 'broad',
    ...         'status': 'paused',
    ...         'cpc_bid': 13.37,
    ...     },
    ... ]
    >>> import pandas as pd
    >>> df = pd.DataFrame.from_dict(objects)
    >>> client.dump_table(df, 'new_objects_table')
    >>> client.sync_objects('new_objects_table')
    >>> client.wait_jobs()

Getting Campaigns Report For an Account
---------------------------------------

    >>> client.get_campaigns_report(7857288943, 'campaigns_test_table', 'CampaignStatus = "PAUSED"',
    ...                             fields=['AccountDescriptiveName', 'CampaignName', 'CampaignStatus'])
    >>> report_df = client.load_table('campaigns_test_table')
    >>> print(type(report_df))
    <class 'pandas.core.frame.DataFrame'>

    >>> print(report_df.to_string(index=False))
    AccountDescriptiveName       CampaignName CampaignStatus
              TestAccount  API test campaign         paused

Getting Keywords Report For an Account
---------------------------------------

    >>> client.get_keywords_report(7857288943, 'keywords_test_table', 'CampaignStatus = "PAUSED"', fields=True)
    >>> report_df = client.load_table('keywords_test_table')
    >>> print(report_df[['AccountDescriptiveName', 'AdGroupName', 'CampaignName', 'Criteria', 'KeywordMatchType', 'CpcBid']].to_string(index=False))
    AccountDescriptiveName       AdGroupName       CampaignName        Criteria KeywordMatchType  CpcBid
              TestAccount  API test adgroup  API test campaign  my search term            Broad   13.37

Setting Keyword Bids
--------------------

    >>> report_df['NewCpcBid'] = 4.20
    >>> table_mappings = {
    ...     'new_bid': 'NewCpcBid',
    ...     'old_bid': 'CpcBid',
    ...     'client_id': 'ExternalCustomerId',
    ...     'campaign_id': 'CampaignId',
    ...     'adgroup_id': 'AdGroupId',
    ...     'keyword_id': 'Id',
    ... }
    >>> client.dump_table(report_df, 'new_keywords_test_table', table_mappings=table_mappings)
    >>> client.modify_bids('new_keywords_test_table')
    >>> client.wait_jobs()


    >>> client.get_keywords_report(7857288943, 'keywords_test_table', 'CampaignStatus = "PAUSED"', fields=True, create_table=True)
    >>> new_report_df = client.load_table('keywords_test_table')
    >>> print(new_report_df[['AccountDescriptiveName', 'AdGroupName', 'CampaignName', 'Criteria', 'KeywordMatchType', 'CpcBid']].to_string(index=False))
    AccountDescriptiveName       AdGroupName       CampaignName        Criteria KeywordMatchType  CpcBid
              TestAccount  API test adgroup  API test campaign  my search term            Broad     4.2


Removing Our Test Capaign
-------------------------

    >>> objects = [
    ...     {
    ...         'object_type': 'campaign',
    ...         'client_id': 7857288943,
    ...         'campaign_id': new_report_df['CampaignId'][0],
    ...         'campaign_name': 'API test campaign',
    ...         'operator': 'SET',
    ...         'status': 'REMOVED',
    ...     }
    ... ]
    >>> df = pd.DataFrame.from_dict(objects)
    >>> client.dump_table(df, 'new_objects_table')
    >>> client.sync_objects('new_objects_table')
    >>> client.wait_jobs()
