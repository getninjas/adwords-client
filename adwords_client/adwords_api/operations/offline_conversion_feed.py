

def add_offline_conversion_feed_operation(google_click_id: 'String' = None,
                      conversion_name: 'String' = None,
                      conversion_time: 'DateTime' = None,
                      conversion_value: 'Double' = 0,
                      conversion_currency_code: 'String' = 'BRL',
                      external_attribution_credit: 'Double' = None,
                      external_attribution_model: 'String' = None,
                      operator: 'String' = 'ADD',
                      time_zone: 'String' = 'America/Sao_Paulo',
                      **kwargs):

    operation = {
        'xsi_type': 'OfflineConversionFeedOperation',
        'operand': {
            # https://developers.google.com/adwords/api/docs/reference/v201705/AdGroupService.AdGroup
            'xsi_type': 'OfflineConversionFeed',
            'googleClickId': google_click_id,
            'conversionName': conversion_name,
            'conversionTime': conversion_time + time_zone,
            'conversionValue': conversion_value,
            'conversionCurrencyCode': conversion_currency_code,

        },
        'operator': operator,
    }
    if external_attribution_credit:
        operation['operand']['externalAttributionCredit'] = external_attribution_credit

    if external_attribution_model:
        operation['operand']['externalAttributionModel'] = external_attribution_model

    return operation


def set_offline_conversion_feed_operation(google_click_id: 'String' = None,
                      conversion_name: 'String' = None,
                      conversion_time: 'DateTime' = None,
                      adjustment_time: 'DateTime' = None,
                      conversion_value: 'Double' = 0,
                      conversion_currency_code: 'String' = 'BRL',
                      operator: 'String' = 'SET',
                      time_zone: 'String' = 'America/Sao_Paulo',
                      **kwargs):
    operation = {
        'xsi_type': 'OfflineConversionAdjustmentFeedOperation',
        'operand': {
            # https://developers.google.com/adwords/api/docs/reference/v201705/AdGroupService.AdGroup
            'xsi_type': 'GclidOfflineConversionAdjustmentFeed',
            'googleClickId': google_click_id,
            'conversionName': conversion_name,
            'conversionTime': conversion_time + time_zone,
            'adjustedValue': conversion_value,
            'adjustedValueCurrencyCode': conversion_currency_code,
            'adjustmentTime': adjustment_time + time_zone,
            'adjustmentType': 'RESTATE'

        },
        'operator': 'ADD',
    }
    if operator == 'REMOVE':
        operation['operand']['adjustmentType'] = 'RETRACT'
    return operation