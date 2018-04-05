class SyncJobService:
    def __init__(self, client):
        self.client = client
        self.services = {}

    def get_service(self, service_name):
        if service_name not in self.services:
            self.services[service_name] = globals()[service_name]
        return self.services[service_name](self.client.client)

    def mutate(self, client_id, operations_list, service_name):
        service = self.get_service(service_name)
        service.prepare_mutate()
        for i, operation in enumerate(operations_list, 1):
            service.helper.add_operation(operation)
        service.mutate(int(client_id))
