import logging

logger = logging.getLogger(__name__)


def get_clicks_report(client, customer_id, *args, **kwargs):
    include_fields = kwargs.pop('include_fields', [])
    exclude_fields = kwargs.pop('exclude_fields', ['ConversionTypeName'])
    exclude_terms = kwargs.pop('exclude_terms', ['Significance'])
    exclude_behavior = kwargs.pop('exclude_behavior', ['Segment'])
    kwargs['include_zero_impressions'] = False
    report = client.get_report(
        'CLICK_PERFORMANCE_REPORT',
        customer_id,
        exclude_fields,
        exclude_terms,
        exclude_behavior,
        include_fields,
        *args, **kwargs
    )
    return report


def get_negative_keywords_report(client, customer_id, *args, **kwargs):
    exclude_fields = ['ConversionTypeName']
    exclude_terms = []
    report = client.get_report(
        'CAMPAIGN_NEGATIVE_KEYWORDS_PERFORMANCE_REPORT',
        customer_id,
        exclude_fields,
        exclude_terms,
        ['Segment'],
        [],
        *args, **kwargs
    )
    return report


def get_criteria_report(client, customer_id, *args, **kwargs):
    exclude_fields = ['ConversionTypeName']
    exclude_terms = ['Significance', 'ActiveView', 'Average']
    report = client.get_report(
        'CRITERIA_PERFORMANCE_REPORT',
        customer_id,
        exclude_fields,
        exclude_terms,
        ['Segment'],
        [],
        *args, **kwargs
    )
    return report


def get_ad_performance_report(client, customer_id, *args, **kwargs):
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
                'CombinedApprovalStatus',
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
    report = client.get_report(
        'AD_PERFORMANCE_REPORT',
        customer_id,
        exclude_fields,
        exclude_terms,
        exclude_behavior,
        include_fields,
        *args, **kwargs
    )
    return report


def get_keywords_report(client, customer_id, *args, **kwargs):
    exclude_fields = kwargs.pop('exclude_fields', ['ConversionTypeName'])
    exclude_terms = kwargs.pop('exclude_terms', ['Significance'])
    include_fields = kwargs.pop('include_fields', [])
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
                'ValuePerAllConversion',
                'ValuePerConversion',
            ]
    report = client.get_report(
        'KEYWORDS_PERFORMANCE_REPORT',
        customer_id,
        exclude_fields,
        exclude_terms,
        ['Segment'],
        include_fields,
        *args, **kwargs
    )
    return report


def get_search_terms_report(client, customer_id, *args, **kwargs):
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
    kwargs['include_zero_impressions'] = False
    report = client.get_report(
        'SEARCH_QUERY_PERFORMANCE_REPORT',
        customer_id,
        exclude_fields,
        exclude_terms,
        ['Segment'],
        [],
        *args, **kwargs
    )
    return report


def get_campaigns_report(client, customer_id, *args, **kwargs):
    include_fields = kwargs.pop('include_fields', [])
    exclude_fields = kwargs.pop('exclude_fields', ['ConversionTypeName'])
    exclude_terms = kwargs.pop('exclude_terms', ['Significance'])
    exclude_behavior = kwargs.pop('exclude_behavior', ['Segment'])
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
                'ServingStatus',
                'BiddingStrategyType',
                'SearchImpressionShare',
            ]
    report = client.get_report(
        'CAMPAIGN_PERFORMANCE_REPORT',
        customer_id,
        exclude_fields,
        exclude_terms,
        exclude_behavior,
        include_fields,
        *args, **kwargs
    )
    return report


def get_labels_report(client, customer_id, *args, **kwargs):
    """
    Get report from AdWords account 'customer_id' and save to Redshift 'target_name' table
    """
    include_fields = kwargs.pop('include_fields', [])
    exclude_fields = kwargs.pop('exclude_fields', [])
    exclude_terms = kwargs.pop('exclude_terms', [])
    exclude_behavior = kwargs.pop('exclude_behavior', ['Segment'])
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
    report = client.get_report(
        'LABEL_REPORT',
        customer_id,
        exclude_fields,
        exclude_terms,
        exclude_behavior,
        include_fields,
        *args, **kwargs
    )
    return report


def get_budget_report(
        client, customer_id, *args, **kwargs):
    """
    Get report from AdWords account 'customer_id' and save to Redshift 'target_name' table
    """
    include_fields = kwargs.pop('include_fields', [])
    exclude_fields = kwargs.pop('exclude_fields', ['ConversionTypeName'])
    exclude_terms = kwargs.pop('exclude_terms', ['Significance'])
    exclude_behavior = kwargs.pop('exclude_behavior', ['Segment'])
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
                'DeliveryMethod'
                'BudgetReferenceCount',  # The number of campaigns actively using the budget.
                'Amount',  # The daily budget
                'IsBudgetExplicitlyShared',
                # Shared budget (true) or specific to the campaign (false)
            ]
    report = client.get_report(
        'BUDGET_PERFORMANCE_REPORT',
        customer_id,
        exclude_fields,
        exclude_terms,
        exclude_behavior,
        include_fields,
        *args, **kwargs
    )
    return report


def get_adgroups_report(client, customer_id, *args, **kwargs):
    include_fields = kwargs.pop('include_fields', [])
    exclude_fields = kwargs.pop('exclude_fields', ['ConversionTypeName'])
    exclude_terms = kwargs.pop('exclude_terms', ['Significance'])
    exclude_behavior = kwargs.pop('exclude_behavior', ['Segment'])
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
                'ValuePerAllConversion',
                'ValuePerConversion',
            ]
    report = client.get_report(
        'ADGROUP_PERFORMANCE_REPORT',
        customer_id,
        exclude_fields,
        exclude_terms,
        exclude_behavior,
        include_fields,
        *args, **kwargs
    )
    return report


def get_campaigns_location_report(client, customer_id, *args, **kwargs):
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
    report = client.get_report(
        'CAMPAIGN_LOCATION_TARGET_REPORT',
        customer_id,
        exclude_fields,
        exclude_terms,
        ['Segment'],
        [],
        *args, **kwargs
    )
    return report
