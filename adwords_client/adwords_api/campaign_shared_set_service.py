from . import common as cm


class CampaignSharedSetService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'CampaignSharedSetService')
