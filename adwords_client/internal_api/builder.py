import logging
from .mappers import cast_to_adwords
from ..adwords_api.operations import campaign, adgroup, keyword, ad, label

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

    def _parse_operation(self, operation):
        if self.valid_operation(operation):
            object_type = operation.pop('object_type')
            operation = self.filter_operation(self.cast_operation(operation))
            if object_type == 'keyword':
                yield from self._parse_keyword(operation)
            elif object_type == 'adgroup':
                yield from self._parse_adgroup(operation)
            elif object_type == 'ad':
                yield from self._parse_ad(operation)
            elif object_type == 'campaign':
                yield from self._parse_campaign(operation)
            elif object_type == 'label':
                yield from self._parse_label(operation)
            # add new internal_operations types here -- such as "shared_set"
            # elif object_type == 'shared_set':
            #     raise NotImplementedError()
            #     yield from self._shared_set(operation)
            else:
                logger.warning('Operation not recognized: {}', operation)
                yield None

    # create a new module under adwords_client.adwords_api.operations named "shared_set" that parses the internal op
    # def _shared_set(self, operation):
    #     raise NotImplementedError()
    #     yield shared_set.new_shared_set_operation(**operation)

    def _parse_keyword(self, operation):
        yield keyword.new_keyword_operation(**operation)

    def _parse_adgroup(self, operation):
        yield adgroup.adgroup_operation(**operation)

    def _parse_ad(self, operation):
        yield ad.expanded_ad_operation(**operation)

    def _parse_campaign(self, operation):
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

    def _parse_label(self, operation):
        yield label.new_label_operation(**operation)

