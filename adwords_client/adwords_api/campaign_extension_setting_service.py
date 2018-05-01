from . import common as cm


class CampaignExtensionSettingService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'CampaignExtensionSettingService')

    def cs_mutate(self, customer_id, operations):
        self.prepare_mutate()
        self.helper.add_operations(operations)
        return self.mutate(customer_id)

    def cs_get(self, internal_operation):
        fields = ['CampaignId','Extensions']
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
        client_id = None
        self.prepare_get()
        if 'client_id' in internal_operation:
            client_id = internal_operation['client_id']
        if 'predicate' in internal_operation:
            predicate.extend(internal_operation['predicate'])
        for predicate_item in predicate:
            self.helper.add_predicate(predicate_item['field'], predicate_item['operator'], predicate_item['values'])
        if 'fields' in internal_operation:
            [fields.append(field) for field in internal_operation['fields'] if field not in fields]
        self.helper.add_fields(*fields)
        return self.get(client_id)
