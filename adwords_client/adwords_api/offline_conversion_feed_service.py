from . import common as cm


class OfflineConversionFeedService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'OfflineConversionFeedService')
