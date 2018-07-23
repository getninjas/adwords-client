from . import common as cm


class OfflineConversionAdjustmentFeedService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'OfflineConversionAdjustmentFeedService')
