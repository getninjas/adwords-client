from . import common as cm


class LabelService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'LabelService')
