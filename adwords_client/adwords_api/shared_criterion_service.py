from . import common as cm


class SharedCriterionService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'SharedCriterionService')

    def add_negative_keywords_to_shared_set(self, keywords, shared_set_id, customer_id=None):
        self.prepare_mutate()
        operations = []
        for keyword in keywords:
            operation = {
                'operator': 'ADD',
                'operand': {
                    'negative': True,
                    'criterion': {
                        'xsi_type': 'Keyword',
                        'text': keyword['text'],
                        'matchType': keyword['match_type'],
                    },
                    'sharedSetId': shared_set_id
                }

            }
            operations.append(operation)
        self.helper.add_operations(operations)
        self.mutate(customer_id)

    def get_negative_keywords_from_list(self, customer_id, shared_set_id, fields):
        predicate = [{'field': 'CriteriaType', 'operator': 'EQUALS', 'values': 'KEYWORD'},
                     {'field': 'SharedSetId', 'operator': 'EQUALS', 'values': shared_set_id},
                     {'field': 'Negative', 'operator': 'EQUALS', 'values': True}]
        self.prepare_get()
        if fields is not None:
            needed_fields = ['Id', 'KeywordText', 'KeywordMatchType']
            [fields.append(field) for field in needed_fields if field not in fields]
        else:
            fields = ['Id', 'KeywordText', 'KeywordMatchType']
        self.helper.add_fields(fields)
        for predicate_item in predicate:
            self.helper.add_predicate(predicate_item['field'], predicate_item['operator'], predicate_item['values'])
        return self.get(customer_id)

    def update_shared_set(self, operations, customer_id=None):
        self.prepare_mutate()
        self.helper.add_operations(operations)
        return self.mutate(customer_id)

    def cs_mutate(self, customer_id, operations):
        self.prepare_mutate()
        self.helper.add_operations(operations)
        return self.mutate(customer_id)

    def cs_get(self, internal_operation):
        fields = ['Id']
        client_id = None
        self.prepare_get()
        if 'client_id' in internal_operation:
            client_id = internal_operation['client_id']
        if 'predicate' in internal_operation:
            for predicate_item in internal_operation['predicate']:
                self.helper.add_predicate(predicate_item['field'], predicate_item['operator'], predicate_item['values'])
        if 'fields' in internal_operation:
            [fields.append(field) for field in internal_operation['fields'] if field not in fields]
        self.helper.add_fields(*fields)
        return self.get(client_id)
