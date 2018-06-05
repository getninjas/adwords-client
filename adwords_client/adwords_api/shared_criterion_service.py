from . import common as cm


class SharedCriterionService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'SharedCriterionService')
