from . import common as cm


class SharedSetService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'SharedSetService')
