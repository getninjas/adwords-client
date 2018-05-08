from . import common as cm
import logging

logger = logging.getLogger(__name__)


class CustomerService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'CustomerService')

    def mutate(self, client_customer_id=None, sync=None):
        if len(self.helper.operations) > 1:
            raise Exception('Only one customer operation is supported per time')
        else:
            operation = self.helper.operations[0]
            customers = self.sync_get(operation={}, client_id=client_customer_id)
            if len(customers) > 0:
                customer = customers[0]
                if 'tracking_url_template' in operation:
                    customer.trackingUrlTemplate = operation['tracking_url_template']
                if 'auto_tagging_enabled' in operation:
                    customer.autoTaggingEnabled = operation['auto_tagging_enabled']
                if 'final_url_suffix' in operation:
                    customer.finalUrlSuffix = operation['final_url_suffix']
                customer.__values__.pop('parallelTrackingEnabled')
                result = self.service.mutate(customer)
                result['returnType'] = 'Customer'
                return [result]
            else:
                raise Exception('No customers with this Id')

    def sync_get(self, operation, client_id=None):
        if client_id:
            self.client.SetClientCustomerId(client_id)
        return self.service.getCustomers()
