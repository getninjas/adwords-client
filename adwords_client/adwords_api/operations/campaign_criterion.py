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
        # https://developers.google.com/adwords/api/docs/reference/v201708/CampaignCriterionService.CampaignCriterionOperation
        'xsi_type': 'CampaignCriterionOperation',
        'operator': operator.upper(),
        'operand': {
            # https://developers.google.com/adwords/api/docs/reference/v201708/CampaignCriterionService.CampaignCriterion
            'xsi_type': 'CampaignCriterion',
            'campaignId': campaign_id,
            'criterion': {
                # https://developers.google.com/adwords/api/docs/reference/v201708/CampaignCriterionService.Language
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
