from . import common as cm


class CampaignCriterionService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'CampaignCriterionService')

    def custom_get(self, internal_operation):
        fields = ['CampaignId']
        predicate = []
        if internal_operation['object_type'] == 'campaign_ad_schedule':
            predicate.append({
                'field': 'CriteriaType', 'operator': 'EQUALS', 'values': 'AD_SCHEDULE'
            })
            fields.extend(['DayOfWeek', 'StartHour', 'StartMinute', 'EndHour', 'EndMinute', 'BidModifier'])
        elif internal_operation['object_type'] == 'campaign_targeted_location':
            predicate.append({
                'field': 'CriteriaType', 'operator': 'EQUALS', 'values': 'LOCATION'
            })
            fields.extend(['LocationName', 'BidModifier', 'Id'])
        elif internal_operation['object_type'] == 'campaign_language':
            predicate.append({
                'field': 'CriteriaType', 'operator': 'EQUALS', 'values': 'LANGUAGE'
            })
            fields.extend(['LanguageName', 'BidModifier', 'Id'])
        self.prepare_get()
        client_id = internal_operation.get('client_id')
        predicate.extend(internal_operation.get('predicate', []))
        fields = set(fields).union(internal_operation.get('fields', []))
        for predicate_item in predicate:
            self.helper.add_predicate(predicate_item['field'], predicate_item['operator'], predicate_item['values'])
        self.helper.add_fields(*fields)
        return self.get(client_id)
