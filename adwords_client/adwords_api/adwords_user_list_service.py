from . import common as cm
import logging

logger = logging.getLogger(__name__)


class CustomSyncReturnValue(cm.SyncReturnValue):
    def __init__(self, callback, parameters):
        member_operations = [adw_op for adw_op in parameters if adw_op['xsi_type'] == 'MutateMembersOperation']
        regular_operations = [adw_op for adw_op in parameters if adw_op['xsi_type'] == 'UserListOperation']

        if member_operations:
            self.result = self._upload_sync_operations(callback.mutateMembers, member_operations)
            self.operations_sent = member_operations
        elif regular_operations:
            self.result = self._upload_sync_operations(callback.mutate, regular_operations)
            self.operations_sent = regular_operations

    def __iter__(self):
        if self.result and 'value' in self.result:
            for entry in self.result.value:
                yield entry
        elif self.result and 'userLists' in self.result:
            for entry in self.result.userLists:
                yield entry


class AdwordsUserListService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'AdwordsUserListService')

    def prepare_mutate(self, sync=None):
        self.helper = cm.SyncServiceHelper(self)
        self.ResultProcessor = CustomSyncReturnValue

    def get(self, operation, client_id=None):
        self.ResultProcessor = cm.PagedResult
        if client_id:
            self.client.SetClientCustomerId(client_id)
        operation = list(operation)
        if operation:
            operation = operation[0]
        else:
            raise RuntimeError('The operation object is empty')
        return self.ResultProcessor(self.service.get, operation)

