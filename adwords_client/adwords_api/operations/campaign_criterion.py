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


def targeted_location_operation(campaign_id: 'Long' = None,
                                location_id: 'Long' = None,
                                bid_modifier: 'Double' = None,
                                operator: 'String' = 'ADD',
                                **kwargs):
    operation = {
        # https://developers.google.com/adwords/api/docs/reference/v201708/CampaignCriterionService.CampaignCriterionOperation
        'xsi_type': 'CampaignCriterionOperation',
        'operator': operator.upper(),
        'operand': {
            # https://developers.google.com/adwords/api/docs/reference/v201708/CampaignCriterionService.CampaignCriterion
            'xsi_type': 'CampaignCriterion',
            'campaignId': campaign_id,
            'criterion': {
                # https://developers.google.com/adwords/api/docs/reference/v201708/CampaignCriterionService.Location
                'xsi_type': 'Location',
                'id': location_id,
            }
        },
    }
    if bid_modifier:
        operation['operand']['bidModifier'] = bid_modifier
    return operation


def get_campaign_criterion_operation(fields: 'list'=None, predicates: 'list'=None, **kwargs):
    predicates = predicates or []
    fields = set(fields or [])
    default_fields = kwargs.pop('default_fields', False)
    object_type = kwargs.pop('object_type', None)
    if object_type == 'campaign_ad_schedule':
        predicates.append(('CriteriaType','EQUALS','AD_SCHEDULE'))
        if default_fields:
            fields.update({'DayOfWeek', 'StartHour', 'StartMinute', 'EndHour', 'EndMinute', 'BidModifier'})
    elif object_type == 'campaign_targeted_location':
        predicates.append(('CriteriaType','EQUALS', 'LOCATION'))
        if default_fields:
            fields.update({'LocationName', 'BidModifier', 'Id'})
    elif object_type == 'campaign_language':
        predicates.append(('CriteriaType','EQUALS', 'LANGUAGE'))
        if default_fields:
            fields.update({'LanguageName', 'BidModifier', 'Id'})
    return _get_selector(fields, predicates)
