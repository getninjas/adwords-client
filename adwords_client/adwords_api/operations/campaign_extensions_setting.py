from .utils import _build_new_bidding_strategy_configuration, _build_money, _get_selector


def sitelink_setting_for_campaign_operation(campaign_id: 'Long' = None,
                                    sitelinks_configuration: 'String[]' = None,
                                    operator: 'String' = 'ADD',
                                    **kwargs):

    operation = {
        'xsi_type': 'CampaignExtensionSettingOperation',
        'operator': operator.upper(),
        'operand': {
            'xsi_type': 'CampaignExtensionSetting',
            'campaignId': campaign_id,
            'extensionType': 'SITELINK',
            'extensionSetting': {
                'extensions': []
            }
        },
    }

    for sitelink in sitelinks_configuration:
        operation['operand']['extensionSetting']['extensions'].append({
            'xsi_type': 'SitelinkFeedItem',
            'sitelinkText': sitelink['sitelink_text'],
            'sitelinkFinalUrls': {'urls': [sitelink['sitelink_final_url']]},
        })
    return operation


def callout_setting_for_campaign_operation(campaign_id: 'Long' = None,
                                    callout_text_list: 'String[]' = None,
                                    operator: 'String' = 'ADD',
                                    **kwargs):
    operation = {
        'xsi_type': 'CampaignExtensionSettingOperation',
        'operator': operator.upper(),
        'operand': {
            'xsi_type': 'CampaignExtensionSetting',
            'campaignId': campaign_id,
            'extensionType': 'CALLOUT',
            'extensionSetting': {
                'extensions': []
            }
        },
    }

    for callout_text in callout_text_list:
        operation['operand']['extensionSetting']['extensions'].append({
            'xsi_type': 'CalloutFeedItem',
            'calloutText': callout_text
        })
    return operation


def structured_snippet_setting_for_campaign_operation(campaign_id: 'Long' = None,
                                               snippets_configuration: 'String[]' = None,
                                               operator: 'String' = 'ADD',
                                               **kwargs):
    operation = {
        'xsi_type': 'CampaignExtensionSettingOperation',
        'operator': operator.upper(),
        'operand': {
            'xsi_type': 'CampaignExtensionSetting',
            'campaignId': campaign_id,
            'extensionType': 'STRUCTURED_SNIPPET',
            'extensionSetting': {
                'extensions': []
            }
        },
    }
    for snippet in snippets_configuration:
        operation['operand']['extensionSetting']['extensions'].append({
            'xsi_type': 'StructuredSnippetFeedItem',
            'header': snippet['header'],
            'values': snippet['values'],
        })

    return operation


def get_campaign_extension_operation(fields: 'list'=None, predicates: 'list'=None, **kwargs):
    default_fields = kwargs.pop('default_fields', False)
    object_type = kwargs.pop('object_type', None)
    predicates = predicates or []
    fields = set(fields or [])
    if default_fields:
        fields.update({'CampaignId', 'Extensions'})
    if object_type == 'campaign_sitelink':
        predicates.append(('ExtensionType', 'EQUALS', 'SITELINK'))
    if object_type == 'campaign_callout':
        predicates.append(('ExtensionType', 'EQUALS', 'CALLOUT'))
    if object_type == 'campaign_structured_snippet':
        predicates.append(('ExtensionType', 'EQUALS', 'STRUCTURED_SNIPPET'))
    return _get_selector(fields, predicates)
