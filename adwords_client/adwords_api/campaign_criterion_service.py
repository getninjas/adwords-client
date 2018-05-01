from . import common as cm


class CampaignCriterionService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'CampaignCriterionService')

    def cs_mutate(self, customer_id, operations):
        self.prepare_mutate()
        self.helper.add_operations(operations)
        return self.mutate(customer_id)

    def cs_get(self, internal_operation):
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
        client_id = None
        self.prepare_get()
        if 'client_id' in internal_operation:
            client_id = internal_operation['client_id']
        if 'predicate' in internal_operation:
            predicate.extend(internal_operation['predicate'])
        if 'fields' in internal_operation:
            [fields.append(field) for field in internal_operation['fields'] if field not in fields]
        for predicate_item in predicate:
            self.helper.add_predicate(predicate_item['field'], predicate_item['operator'], predicate_item['values'])
        self.helper.add_fields(*fields)
        return self.get(client_id)
