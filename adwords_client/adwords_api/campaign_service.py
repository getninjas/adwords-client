from . import common as cm


class CampaignService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'CampaignService')

