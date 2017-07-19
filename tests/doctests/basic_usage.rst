===========
Basic Usage
===========

Client creation
---------------

    >>> from adwords_client.client import AdWords
    >>> client = AdWords.autoload()

.. testsetup:

    >>> client.get_keywords_report(3709730243, 'setup_keywords_test_table', fields=True)
    >>> report_df = client.load_table('setup_keywords_test_table')
    >>> report_df['NewCpcBid'] = 13.37
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
    >>> client.exponential_backoff()


Getting Campaigns Report For an Account
---------------------------------------

    >>> client.get_campaigns_report(3709730243, 'campaigns_test_table', fields=True)
    >>> report_df = client.load_table('campaigns_test_table')
    >>> type(report_df)
    <class 'pandas.core.frame.DataFrame'>
    >>> print(report_df.sort_values('CampaignId').to_string(index=False))
    AccountDescriptiveName BiddingStrategyType  CampaignId   CampaignName CampaignStatus  Clicks  Conversions  Cost  ExternalCustomerId  Impressions  SearchImpressionShare
        AdwordsClientTest     Maximize clicks   886235670  TestCampaign1        enabled       0          0.0   0.0          3709730243            0                    0.0

Getting Keywords Report For an Account
---------------------------------------

    >>> client.get_keywords_report(3709730243, 'keywords_test_table', fields=True)
    >>> report_df = client.load_table('keywords_test_table')
    >>> print(report_df.sort_values(['Id', 'AdGroupId']).to_string(index=False))
    AccountDescriptiveName    AdGroupId   AdGroupName AdGroupStatus BiddingStrategySource BiddingStrategyType  CampaignId   CampaignName CampaignStatus  Clicks  Conversions  Cost  CpcBid CreativeQualityScore Criteria  ExternalCustomerId            Id  Impressions KeywordMatchType PostClickQualityScore  QualityScore  SearchImpressionShare SearchPredictedCtr   Status
        AdwordsClientTest  50068824411  TestAdgroup1       enabled              campaign     Maximize clicks   886235670  TestCampaign1        enabled       0          0.0   0.0   13.37                   --    test3          3709730243  296032439410            0            Broad                    --             0                    0.0                 --  enabled
        AdwordsClientTest  50068824411  TestAdgroup1       enabled              campaign     Maximize clicks   886235670  TestCampaign1        enabled       0          0.0   0.0   13.37                   --    test1          3709730243  296259232243            0            Broad                    --             0                    0.0                 --  enabled
        AdwordsClientTest  50068824411  TestAdgroup1       enabled              campaign     Maximize clicks   886235670  TestCampaign1        enabled       0          0.0   0.0   13.37                   --    test2          3709730243  296259232283            0            Broad                    --             0                    0.0                 --  enabled

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
    >>> client.exponential_backoff()
    >>> client.get_keywords_report(3709730243, 'keywords_test_table', fields=True, create_table=True)
    >>> new_report_df = client.load_table('keywords_test_table')
    >>> print(new_report_df.sort_values(['Id', 'AdGroupId']).to_string(index=False))
    AccountDescriptiveName    AdGroupId   AdGroupName AdGroupStatus BiddingStrategySource BiddingStrategyType  CampaignId   CampaignName CampaignStatus  Clicks  Conversions  Cost  CpcBid CreativeQualityScore Criteria  ExternalCustomerId            Id  Impressions KeywordMatchType PostClickQualityScore  QualityScore  SearchImpressionShare SearchPredictedCtr   Status
        AdwordsClientTest  50068824411  TestAdgroup1       enabled              campaign     Maximize clicks   886235670  TestCampaign1        enabled       0          0.0   0.0     4.2                   --    test3          3709730243  296032439410            0            Broad                    --             0                    0.0                 --  enabled
        AdwordsClientTest  50068824411  TestAdgroup1       enabled              campaign     Maximize clicks   886235670  TestCampaign1        enabled       0          0.0   0.0     4.2                   --    test1          3709730243  296259232243            0            Broad                    --             0                    0.0                 --  enabled
        AdwordsClientTest  50068824411  TestAdgroup1       enabled              campaign     Maximize clicks   886235670  TestCampaign1        enabled       0          0.0   0.0     4.2                   --    test2          3709730243  296259232283            0            Broad                    --             0                    0.0                 --  enabled
