from .utils import _get_selector


def ad_schedule_operation(campaign_id: 'Long' = None,
                    day_of_week: 'String' = None,
                    start_hour: 'Integer' = None,
                    start_minute: 'String' = None,
                    end_hour: 'Integer' = None,
                    end_minute: 'String' = None,
                    bid_modifier: 'Double' = None,
                    operator: 'String' = 'ADD',
                    **kwargs):
    operation = {
        'xsi_type': 'CampaignCriterionOperation',
        'operator': operator.upper(),
        'operand': {
            'xsi_type': 'CampaignCriterion',
            'campaignId': campaign_id,
            'criterion': {
                'xsi_type': 'AdSchedule',
                'dayOfWeek': day_of_week,
                'startHour': start_hour,
                'startMinute': start_minute,
                'endHour': end_hour,
                'endMinute': end_minute
            }
        },
    }
    if bid_modifier:
        operation['operand']['bidModifier'] = bid_modifier
    return operation


def get_campaign_criterion_operation(object_type, fields=[], predicates=[], **kwargs):
    fields = set(fields).union(['CampaignId'])
    if object_type == 'campaign_ad_schedule':
        predicates.append({'field': 'CriteriaType', 'operator': 'EQUALS', 'values': 'AD_SCHEDULE'})
        fields.union({'DayOfWeek', 'StartHour', 'StartMinute', 'EndHour', 'EndMinute', 'BidModifier'})
    elif object_type == 'campaign_targeted_location':
        predicates.append({'field': 'CriteriaType', 'operator': 'EQUALS', 'values': 'LOCATION'})
        fields.union({'LocationName', 'BidModifier', 'Id'})
    elif object_type == 'campaign_language':
        predicates.append({'field': 'CriteriaType', 'operator': 'EQUALS', 'values': 'LANGUAGE'})
        fields.union({'LanguageName', 'BidModifier', 'Id'})
    return _get_selector(fields, predicates)
