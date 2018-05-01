from . import common as cm
import logging

logger = logging.getLogger(__name__)

class CustomerService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'CustomerService')

    def custom_mutate(self, customer_id, operations):
        if len(operations) > 1:
            raise Exception('Only one customer operation is supported per time')
        else:
            operation = operations[0]
            customers = self.custom_get(operation)
            if len(customers) > 0:
                customer = customers[0]
                if 'tracking_url_template' in operation:
                    customer.trackingUrlTemplate = operation['tracking_url_template']
                if 'auto_tagging_enabled' in operation:
                    customer.autoTaggingEnabled = operation['auto_tagging_enabled']
                if 'final_url_suffix' in operation:
                    customer.finalUrlSuffix = operation['final_url_suffix']
                customer.__values__.pop('parallelTrackingEnabled')
                return self.service.mutate(customer)
            else:
                raise Exception('No customers with this Id')

    def custom_get(self, internal_operation):
        client_id = internal_operation.get('client_id')
        if internal_operation['object_type'] != 'customer':
            raise NotImplementedError()
        if client_id:
            self.client.SetClientCustomerId(client_id)
        return self.service.getCustomers()
