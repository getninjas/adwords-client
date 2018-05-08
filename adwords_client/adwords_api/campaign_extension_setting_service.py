from . import common as cm


class CampaignExtensionSettingService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'CampaignExtensionSettingService')