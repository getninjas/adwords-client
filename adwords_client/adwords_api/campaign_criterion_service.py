from . import common as cm


class CampaignCriterionService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'CampaignCriterionService')
