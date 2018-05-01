from . import common as cm


class CampaignExtensionSettingService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'CampaignExtensionSettingService')

    def custom_get(self, internal_operation):
        fields = ['CampaignId', 'Extensions']
        predicate = []
        if internal_operation['object_type'] == 'campaign_sitelink':
            predicate.append({
                'field': 'ExtensionType', 'operator': 'EQUALS', 'values': 'SITELINK'
            })
        if internal_operation['object_type'] == 'campaign_callout':
            predicate.append({
                'field': 'ExtensionType', 'operator': 'EQUALS', 'values': 'CALLOUT'
            })
        if internal_operation['object_type'] == 'campaign_structured_snippet':
            predicate.append({
                'field': 'ExtensionType', 'operator': 'EQUALS', 'values': 'STRUCTURED_SNIPPET'
            })
        self.prepare_get()
        client_id = internal_operation.get('client_id')
        predicate.extend(internal_operation.get('predicate', []))
        for predicate_item in predicate:
            self.helper.add_predicate(predicate_item['field'], predicate_item['operator'], predicate_item['values'])
        fields = set(fields).union(internal_operation.get('fields', []))
        self.helper.add_fields(*fields)
        return self.get(client_id)
