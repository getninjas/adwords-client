===========
Basic Usage
===========

Client creation
---------------

    >>> from adwords_client.client import AdWords
    >>> client = AdWords('tests/googleads_test.yaml')

Getting Campaigns Report For an Account
---------------------------------------

    >>> client.get_campaigns_report(3709730243, 'campaigns_test_table')
    >>> report_df = client.load_table('campaigns_test_table')
    >>> type(report_df)
    <class 'pandas.core.frame.DataFrame'>
    >>> report_df.shape
    (1, 105)

Getting Keywords Report For an Account
---------------------------------------

    >>> client.get_keywords_report(3709730243, 'keywords_test_table')
    >>> report_df = client.load_table('keywords_test_table')
    >>> report_df.shape
    (3, 111)
