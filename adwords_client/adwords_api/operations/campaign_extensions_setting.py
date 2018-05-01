

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
