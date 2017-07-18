===========
Basic Usage
===========

Client creation
---------------

    >>> from adwords_client.client import AdWords
    >>> client = AdWords.autoload()

Getting Campaigns Report For an Account
---------------------------------------

    >>> client.get_campaigns_report(3709730243, 'campaigns_test_table', fields=True)
    >>> report_df = client.load_table('campaigns_test_table')
    >>> type(report_df)
    <class 'pandas.core.frame.DataFrame'>
    >>> print(report_df.to_string())
      AccountDescriptiveName BiddingStrategyType   CampaignId   CampaignName CampaignStatus  Clicks  Conversions  Cost  ExternalCustomerId  Impressions  SearchImpressionShare
    0      AdwordsClientTest     Maximize clicks  886235670.0  TestCampaign1        enabled     0.0          0.0   0.0        3.709730e+09          0.0                    0.0

Getting Keywords Report For an Account
---------------------------------------

    >>> client.get_keywords_report(3709730243, 'keywords_test_table', fields=True)
    >>> report_df = client.load_table('keywords_test_table')
    >>> print(report_df.to_string())
      AccountDescriptiveName     AdGroupId   AdGroupName AdGroupStatus BiddingStrategySource BiddingStrategyType   CampaignId   CampaignName CampaignStatus  Clicks  Conversions  Cost     CpcBid CreativeQualityScore Criteria  ExternalCustomerId            Id  Impressions KeywordMatchType PostClickQualityScore  QualityScore  SearchImpressionShare SearchPredictedCtr   Status
    0      AdwordsClientTest  5.006882e+10  TestAdgroup1       enabled              campaign     Maximize clicks  886235670.0  TestCampaign1        enabled     0.0          0.0   0.0  6690000.0                   --    test3        3.709730e+09  2.960324e+11          0.0            Broad                    --           0.0                    0.0                 --  enabled
    1      AdwordsClientTest  5.006882e+10  TestAdgroup1       enabled              campaign     Maximize clicks  886235670.0  TestCampaign1        enabled     0.0          0.0   0.0  6690000.0                   --    test1        3.709730e+09  2.962592e+11          0.0            Broad                    --           0.0                    0.0                 --  enabled
    2      AdwordsClientTest  5.006882e+10  TestAdgroup1       enabled              campaign     Maximize clicks  886235670.0  TestCampaign1        enabled     0.0          0.0   0.0  6690000.0                   --    test2        3.709730e+09  2.962592e+11          0.0            Broad                    --           0.0                    0.0                 --  enabled
