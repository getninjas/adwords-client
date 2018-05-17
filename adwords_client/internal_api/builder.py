import logging
from .mappers import cast_to_adwords
from ..adwords_api.operations import (campaign, adgroup, keyword, ad, label, campaign_shared_set, shared_criterion,
                                      shared_set, managed_customer, budget_order, attach_label,
                                      campaign_extensions_setting, campaign_criterion, utils)

logger = logging.getLogger(__name__)


class OperationsBuilder:
    def __init__(self, min_id=0):
        self.min_id = min_id
        self.remove_operations = {
            'campaign': set(),
            'adgroup': set(),
        }

    def __call__(self, *args, **kwargs):
        return self._parse_operation(*args, **kwargs)

    def get_next_id(self):
        self.min_id -= 1
        return self.min_id

    def valid_operation(self, operation):
        is_remove = False
        logger.debug('Internal Operation: %s', str(operation))
        if operation.get('operator', '').upper() == 'REMOVE' or operation.get('status', '').upper() == 'REMOVED':
            is_remove = True
        if is_remove:
            logger.debug('Remove operation')
            campaign_id = operation.get('campaign_id')
            logger.debug('Campaign id: %s', str(campaign_id))
            if operation['object_type'] == 'campaign':
                if not campaign_id:
                    raise RuntimeError('Campaign operation without campaign_id')
                self.remove_operations['campaign'].add(campaign_id)
                return True
            if campaign_id in self.remove_operations['campaign']:
                return False
            adgroup_id = operation.get('adgroup_id')
            logger.debug('Adgroup id: %s', str(adgroup_id))
            if operation['object_type'] == 'adgroup':
                if not adgroup_id:
                    raise RuntimeError('Adgroup operation without adgroup_id')
                self.remove_operations['adgroup'].add(adgroup_id)
                return True
            if adgroup_id in self.remove_operations['adgroup']:
                return False
        logger.debug('Non remove operation')
        return True

    def cast_operation(self, operation):
        return {k: cast_to_adwords(k, v) for k, v in operation.items()}

    def filter_operation(self, operation):
        return {k: v for k, v in operation.items() if v is not None}

    def _parse_operation(self, operation, sync=None):
        if self.valid_operation(operation):
            object_type = operation.get('object_type')
            operation = self.filter_operation(self.cast_operation(operation))
            if object_type == 'keyword':
                yield from self._parse_keyword(operation)
            elif object_type == 'adgroup':
                yield from self._parse_adgroup(operation)
            elif object_type == 'ad':
                yield from self._parse_ad(operation)
            elif object_type == 'campaign':
                yield from self._parse_campaign(operation, sync)
            elif object_type == 'label':
                yield from self._parse_label(operation)
            elif object_type == 'managed_customer' and sync:
                yield from self._parse_managed_customer(operation)
            elif object_type == 'customer':
                yield from self._parse_customer(operation)
            elif object_type == 'shared_criterion':
                yield from self._parse_shared_criterion(operation)
            elif object_type == 'campaign_shared_set':
                yield from self._parse_campaign_shared_set(operation)
            elif object_type == 'shared_set':
                yield from self._parse_shared_set(operation)
            elif object_type == 'budget_order' and sync:
                yield from self._parse_budget_order(operation)
            elif object_type == 'attach_label' and sync:
                yield from self._parse_attach_label(operation)
            elif object_type == 'campaign_sitelink':
                yield from self._parse_sitelinks_setting_for_campaign(operation)
            elif object_type == 'campaign_structured_snippet':
                yield from self._parse_structured_snippets_setting_for_campaign(operation)
            elif object_type == 'campaign_callout':
                yield from self._parse_callouts_setting_for_campaign(operation)
            elif object_type in ['campaign_ad_schedule', 'campaign_targeted_location', 'campaign_language']:
                yield from self._parse_campaign_criterion_operation(operation)
            elif object_type == 'billing_account' and sync:
                yield {}
            elif object_type == 'batch_job':
                yield from self._parse_batch_job_operation(operation)
            else:
                logger.warning('Operation not recognized: {}', operation)
                yield None

    def _parse_batch_job_operation(self, operation):
        if 'fields' in operation or 'default_fields' in operation:
            yield utils.get_batch_job_operation(**operation)
        else:
            raise NotImplementedError()

    def _parse_campaign_criterion_operation(self, operation):
        if 'fields' in operation or 'default_fields' in operation:
            yield campaign_criterion.get_campaign_criterion_operation(**operation)
        elif operation['object_type'] == 'campaign_ad_schedule':
            yield campaign_criterion.ad_schedule_operation(**operation)
        else:
            raise NotImplementedError()

    def _parse_callouts_setting_for_campaign(self, operation):
        if 'fields' in operation or 'default_fields' in operation:
            yield campaign_extensions_setting.get_campaign_extension_operation(**operation)
        else:
            yield campaign_extensions_setting.callout_setting_for_campaign_operation(**operation)

    def _parse_structured_snippets_setting_for_campaign(self, operation):
        if 'fields' in operation or 'default_fields' in operation:
            yield campaign_extensions_setting.get_campaign_extension_operation(**operation)
        else:
            yield campaign_extensions_setting.structured_snippet_setting_for_campaign_operation(**operation)

    def _parse_sitelinks_setting_for_campaign(self, operation):
        if 'fields' in operation or 'default_fields' in operation:
            yield campaign_extensions_setting.get_campaign_extension_operation(**operation)
        else:
            yield campaign_extensions_setting.sitelink_setting_for_campaign_operation(**operation)

    def _parse_attach_label(self, operation):
        if 'fields' in operation or 'default_fields' in operation:
            raise TypeError('There is not get method for this object type')
        else:
            yield attach_label.attach_label_operation(**operation)

    def _parse_shared_set(self, operation):
        if 'fields' in operation or 'default_fields' in operation:
            yield shared_set.get_shared_set_operation(**operation)
        else:
            yield shared_set.shared_set_operation(**operation)

    def _parse_campaign_shared_set(self, operation):
        if 'fields' in operation or 'default_fields' in operation:
            yield campaign_shared_set.get_campaign_shared_set(**operation)
        else:
            yield campaign_shared_set.campaign_shared_set_operation(**operation)

    def _parse_shared_criterion(self, operation):
        if 'fields' in operation or 'default_fields' in operation:
            yield shared_criterion.get_shared_criterion_operation(**operation)
        else:
            yield shared_criterion.shared_criterion_operation(**operation)

    def _parse_managed_customer(self, operation):
        if 'fields' in operation or 'default_fields' in operation:
            yield managed_customer.get_managed_customer_operation(**operation)
        else:
            yield managed_customer.managed_customer_operation(**operation)

    def _parse_customer(self, operation):
        if 'fields' in operation or 'default_fields' in operation:
            operation = {}
        else:
            operation['object_type'] = 'customer'
        yield operation

    def _parse_budget_order(self, operation):
        if 'fields' in operation or 'default_fields' in operation:
            yield budget_order.get_budget_order_operation(**operation)
        else:
            yield budget_order.budget_order_operation(**operation)

    def _parse_keyword(self, operation):
        if 'fields' in operation or 'default_fields' in operation:
            keyword.get_keyword_operation(**operation)
        else:
            yield keyword.new_keyword_operation(**operation)

    def _parse_adgroup(self, operation):
        if 'fields' in operation or 'default_fields' in operation:
            yield adgroup.get_ad_group_operation(**operation)
        else:
            yield adgroup.adgroup_operation(**operation)

    def _parse_ad(self, operation):
        if 'fields' in operation or 'default_fields' in operation:
            yield ad.get_ad_operation(**operation)
        else:
            yield ad.expanded_ad_operation(**operation)

    def _parse_campaign(self, operation, sync=None):
        if 'fields' in operation or 'default_fields' in operation:
            yield campaign.get_campaign_operation(**operation)
        elif sync is None:
            if operation.get('operator', 'ADD').upper() == 'ADD' and 'budget_id' not in operation:
                operation['budget_id'] = cast_to_adwords('budget_id', self.get_next_id())
                yield campaign.add_budget(**operation)
            yield campaign.campaign_operation(**operation)
            for language_id in operation.get('languages', []):
                language_id = cast_to_adwords('language_id', language_id)
                yield campaign.add_campaign_language(language_id=language_id, **operation)
            for location_id in operation.get('locations', []):
                location_id = cast_to_adwords('location_id', location_id)
                yield campaign.add_campaign_location(location_id=location_id, **operation)
        else:
            raise RuntimeError('Sync mutate operation ĩs not supported')

    def _parse_label(self, operation):
        if 'fields' in operation or 'default_fields' in operation:
            yield label.get_label_operation(**operation)
        else:
            yield label.new_label_operation(**operation)
