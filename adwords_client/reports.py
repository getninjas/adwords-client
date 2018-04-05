import logging

logger = logging.getLogger(__name__)


def get_clicks_report(client, customer_id, target_name, *args, **kwargs):
    include_fields = kwargs.pop('include_fields', [])
    exclude_fields = kwargs.pop('exclude_fields', ['ConversionTypeName'])
    exclude_terms = kwargs.pop('exclude_terms', ['Significance'])
    exclude_behavior = kwargs.pop('exclude_behavior', ['Segment'])
    create_table = kwargs.pop('create_table', False)
    kwargs['include_zero_impressions'] = False
    return client.get_report('CLICK_PERFORMANCE_REPORT',
                             customer_id,
                             target_name,
                             create_table,
                             exclude_fields,
                             exclude_terms,
                             exclude_behavior,
                             include_fields,
                             *args, **kwargs)


def get_negative_keywords_report(client, customer_id, target_name, *args, **kwargs):
    create_table = kwargs.pop('create_table', False)
    exclude_fields = ['ConversionTypeName']
    exclude_terms = []
    return client.get_report('CAMPAIGN_NEGATIVE_KEYWORDS_PERFORMANCE_REPORT',
                             customer_id,
                             target_name,
                             create_table,
                             exclude_fields,
                             exclude_terms,
                             ['Segment'],
                             [],
                             *args, **kwargs)


def get_criteria_report(client, customer_id, target_name, *args, **kwargs):
    create_table = kwargs.pop('create_table', False)
    exclude_fields = ['ConversionTypeName']
    exclude_terms = ['Significance', 'ActiveView', 'Average']
    return client.get_report('CRITERIA_PERFORMANCE_REPORT',
                             customer_id,
                             target_name,
                             create_table,
                             exclude_fields,
                             exclude_terms,
                             ['Segment'],
                             [],
                             *args, **kwargs)


def get_ad_performance_report(client, customer_id, target_name, *args, **kwargs):
    logger.debug('Running get_ad_performance_report...')
    include_fields = kwargs.pop('include_fields', [])
    exclude_fields = kwargs.pop('exclude_fields', ['BusinessName',
                                                   'ConversionTypeName',
                                                   'CriterionType',
                                                   'CriterionId',
                                                   'ClickType',
                                                   'ConversionCategoryName',
                                                   'ConversionTrackerId',
                                                   'IsNegative'])
    exclude_terms = kwargs.pop('exclude_terms', ['Significance', 'ActiveView', 'Average'])
    exclude_behavior = kwargs.pop('exclude_behavior', ['Segment'])
    create_table = kwargs.pop('create_table', False)
    use_fields = kwargs.pop('fields', False)
    if use_fields:
        try:
            kwargs['fields'] = list(use_fields)
        except TypeError:
            kwargs['fields'] = [
                'AccountDescriptiveName',
                'ExternalCustomerId',
                'CampaignId',
                'CampaignName',
                'CampaignStatus',
                'AdGroupId',
                'AdGroupName',
                'AdGroupStatus',
                'Id',
                'Impressions',
                'Clicks',
                'Conversions',
                'Cost',
                'Status',
                'CreativeUrlCustomParameters',
                'CreativeTrackingUrlTemplate',
                'CreativeDestinationUrl',
                'CreativeFinalUrls',
                'CreativeFinalMobileUrls',
                'CreativeFinalAppUrls',
                'Headline',
                'HeadlinePart1',
                'HeadlinePart2',
                'Description',
                'Description1',
                'Description2',
                'Path1',
                'Path2',
                'DisplayUrl',
            ]
    return client.get_report('AD_PERFORMANCE_REPORT',
                             customer_id,
                             target_name,
                             create_table,
                             exclude_fields,
                             exclude_terms,
                             exclude_behavior,
                             include_fields,
                             *args, **kwargs)


def get_keywords_report(client, customer_id, target_name, *args, **kwargs):
    create_table = kwargs.pop('create_table', False)
    exclude_fields = ['ConversionTypeName']
    exclude_terms = ['Significance']
    use_fields = kwargs.pop('fields', False)
    if use_fields:
        try:
            kwargs['fields'] = list(use_fields)
        except TypeError:
            kwargs['fields'] = [
                'AccountDescriptiveName',
                'ExternalCustomerId',
                'CampaignId',
                'CampaignName',
                'CampaignStatus',
                'AdGroupId',
                'AdGroupName',
                'AdGroupStatus',
                'Id',
                'Impressions',
                'Clicks',
                'Conversions',
                'Cost',
                'Status',
                'KeywordMatchType',
                'Criteria',
                'BiddingStrategySource',
                'BiddingStrategyType',
                'SearchImpressionShare',
                'CpcBid',
                'CreativeQualityScore',
                'PostClickQualityScore',
                'SearchPredictedCtr',
                'QualityScore',
            ]
    return client.get_report('KEYWORDS_PERFORMANCE_REPORT',
                             customer_id,
                             target_name,
                             create_table,
                             exclude_fields,
                             exclude_terms,
                             ['Segment'],
                             [],
                             *args, **kwargs)


def get_search_terms_report(client, customer_id, target_name, *args, **kwargs):
    create_table = kwargs.pop('create_table', False)
    exclude_fields = ['ConversionTypeName']
    exclude_terms = ['Significance']
    use_fields = kwargs.pop('fields', False)
    if use_fields:
        try:
            kwargs['fields'] = list(use_fields)
        except TypeError:
            kwargs['fields'] = [
                'AccountDescriptiveName',
                'ExternalCustomerId',
                'CampaignId',
                'CampaignName',
                'CampaignStatus',
                'AdGroupId',
                'AdGroupName',
                'AdGroupStatus',
                'Impressions',
                'Clicks',
                'Conversions',
                'Cost',
                'Status',
                'KeywordId',
                'KeywordTextMatchingQuery',
                'Query',
            ]
    return client.get_report('SEARCH_QUERY_PERFORMANCE_REPORT',
                             customer_id,
                             target_name,
                             create_table,
                             exclude_fields,
                             exclude_terms,
                             ['Segment'],
                             [],
                             *args, **kwargs)


def get_campaigns_report(client, customer_id, target_name, *args, **kwargs):
    include_fields = kwargs.pop('include_fields', [])
    exclude_fields = kwargs.pop('exclude_fields', ['ConversionTypeName'])
    exclude_terms = kwargs.pop('exclude_terms', ['Significance'])
    exclude_behavior = kwargs.pop('exclude_behavior', ['Segment'])
    create_table = kwargs.pop('create_table', False)
    use_fields = kwargs.pop('fields', False)
    if use_fields:
        try:
            kwargs['fields'] = list(use_fields)
        except TypeError:
            kwargs['fields'] = [
                'AccountDescriptiveName',
                'ExternalCustomerId',
                'CampaignId',
                'CampaignName',
                'CampaignStatus',
                'Impressions',
                'Clicks',
                'Conversions',
                'Cost',
                'Status',
                'BiddingStrategyType',
                'SearchImpressionShare',
            ]
    return client.get_report('CAMPAIGN_PERFORMANCE_REPORT',
                             customer_id,
                             target_name,
                             create_table,
                             exclude_fields,
                             exclude_terms,
                             exclude_behavior,
                             include_fields,
                             *args, **kwargs)


def get_labels_report(client, customer_id, target_name, *args, **kwargs):
    """
    Get report from AdWords account 'customer_id' and save to Redshift 'target_name' table
    """
    include_fields = kwargs.pop('include_fields', [])
    exclude_fields = kwargs.pop('exclude_fields', [])
    exclude_terms = kwargs.pop('exclude_terms', [])
    exclude_behavior = kwargs.pop('exclude_behavior', ['Segment'])
    create_table = kwargs.pop('create_table', False)
    use_fields = kwargs.pop('fields', False)
    if use_fields:
        try:
            kwargs['fields'] = list(use_fields)
        except TypeError:
            kwargs['fields'] = [
                'AccountDescriptiveName',  # The descriptive name of the Customer account.
                'ExternalCustomerId',  # The Customer ID.
                'LabelId',
                'LabelName',
            ]
    return client.get_report('LABEL_REPORT',
                             customer_id,
                             target_name,
                             create_table,
                             exclude_fields,
                             exclude_terms,
                             exclude_behavior,
                             include_fields,
                             *args, **kwargs)


def get_budget_report(client, customer_id, target_name, *args, **kwargs):
    """
    Get report from AdWords account 'customer_id' and save to Redshift 'target_name' table
    """
    include_fields = kwargs.pop('include_fields', [])
    exclude_fields = kwargs.pop('exclude_fields', ['ConversionTypeName'])
    exclude_terms = kwargs.pop('exclude_terms', ['Significance'])
    exclude_behavior = kwargs.pop('exclude_behavior', ['Segment'])
    create_table = kwargs.pop('create_table', False)
    use_fields = kwargs.pop('fields', False)
    if use_fields:
        try:
            kwargs['fields'] = list(use_fields)
        except TypeError:
            kwargs['fields'] = [
                'AccountDescriptiveName',  # The descriptive name of the Customer account.
                'ExternalCustomerId',  # The Customer ID.
                'BudgetId',
                'BudgetName',
                'BudgetReferenceCount',  # The number of campaigns actively using the budget.
                'Amount',  # The daily budget
                'IsBudgetExplicitlyShared',
                # Shared budget (true) or specific to the campaign (false)
            ]
    return client.get_report('BUDGET_PERFORMANCE_REPORT',
                             customer_id,
                             target_name,
                             create_table,
                             exclude_fields,
                             exclude_terms,
                             exclude_behavior,
                             include_fields,
                             *args, **kwargs)


def get_adgroups_report(client, customer_id, target_name, *args, **kwargs):
    include_fields = kwargs.pop('include_fields', [])
    exclude_fields = kwargs.pop('exclude_fields', ['ConversionTypeName'])
    exclude_terms = kwargs.pop('exclude_terms', ['Significance'])
    exclude_behavior = kwargs.pop('exclude_behavior', ['Segment'])
    create_table = kwargs.pop('create_table', False)
    use_fields = kwargs.pop('fields', False)
    if use_fields:
        try:
            kwargs['fields'] = list(use_fields)
        except TypeError:
            kwargs['fields'] = [
                'AccountDescriptiveName',
                'ExternalCustomerId',
                'CampaignId',
                'CampaignName',
                'CampaignStatus',
                'AdGroupId',
                'AdGroupName',
                'AdGroupStatus',
                'Impressions',
                'Clicks',
                'Conversions',
                'Cost',
                'Status',
                'BiddingStrategySource',
                'BiddingStrategyType',
                'SearchImpressionShare',
                'CpcBid',
                'Labels',
            ]
    return client.get_report('ADGROUP_PERFORMANCE_REPORT',
                             customer_id,
                             target_name,
                             create_table,
                             exclude_fields,
                             exclude_terms,
                             exclude_behavior,
                             include_fields,
                             *args, **kwargs)


def get_campaigns_location_report(client, customer_id, target_name, *args, **kwargs):
    create_table = kwargs.pop('create_table', False)
    exclude_fields = []
    exclude_terms = ['Significance']
    use_fields = kwargs.pop('fields', False)
    if use_fields:
        try:
            kwargs['fields'] = list(use_fields)
        except TypeError:
            kwargs['fields'] = [
                'AccountDescriptiveName',
                'ExternalCustomerId',
                'CampaignId',
                'CampaignName',
                'CampaignStatus',
                'Id',
                'IsNegative',
                'BidModifier',
                'Impressions',
                'Clicks',
                'Conversions',
                'Cost',
            ]
    return client.get_report('CAMPAIGN_LOCATION_TARGET_REPORT',
                             customer_id,
                             target_name,
                             create_table,
                             exclude_fields,
                             exclude_terms,
                             ['Segment'],
                             [],
                             *args, **kwargs)
