from . import common as cm


class LabelServiceOperations:
    def __init__(self, label_service):
        self.label_service = label_service
        self.suds_client = label_service.service.suds_client
        self.operations = []

    def add_operation(self, operation):
        self.operations.append(operation)

    def upload_operations(self):
        # TODO make it the same as the one in batch job service
        raise NotImplementedError()


class LabelService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'LabelService')

    def prepare_mutate(self):
        self.helper = LabelServiceOperations(self)
        self.ResultProcessor = cm.SimpleReturnValue


