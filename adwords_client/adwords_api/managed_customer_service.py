from . import common as cm


class ManagedCustomerService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'ManagedCustomerService')
