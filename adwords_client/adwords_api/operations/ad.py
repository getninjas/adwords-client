from .utils import _get_selector

def expanded_ad_operation(adgroup_id: 'Long' = None,
                          ad_id: 'Long' = None,
                          status: 'String' = 'PAUSED',
                          operator: 'String' = 'ADD',
                          headline_part_1: 'String' = '',
                          headline_part_2: 'String' = '',
                          description: 'String' = '',
                          path_1: 'String' = '',
                          path_2: 'String' = '',
                          tracking_url_template: 'String' = None,
                          url_customer_parameters: 'String' = None,
                          final_urls: 'String' = None,
                          final_mobile_urls: 'String' = None,
                          final_app_urls: 'String' = None,
                          **kwargs):
    if operator.upper() == 'ADD':
        ad = _expanded_text_ad(
                    headline_part_1,
                    headline_part_2,
                    description,
                    path_1,
                    path_2,
                    tracking_url_template,
                    url_customer_parameters,
                    final_urls,
                    final_mobile_urls,
                    final_app_urls,
                )
    else:
        ad = _abstract_ad(ad_id)
    operation = {
        'xsi_type': 'AdGroupAdOperation',
        'operand': {
            # https://developers.google.com/adwords/api/docs/reference/v201708/AdGroupAdService.AdGroupAd
            'xsi_type': 'AdGroupAd',
            'adGroupId': adgroup_id,
            'ad': ad,
            'status': status,
        },
        'operator': operator,
    }
    return operation


def _expanded_text_ad(headline_part_1='',
                      headline_part_2='',
                      description='',
                      path_1='',
                      path_2='',
                      tracking_url_template=None,
                      url_customer_parameters=None,
                      final_urls=None,
                      final_mobile_urls=None,
                      final_app_urls=None):
    # https://developers.google.com/adwords/api/docs/reference/v201708/AdGroupAdService.Ad
    # https://developers.google.com/adwords/api/docs/reference/v201708/AdGroupAdService.ExpandedTextAd
    ad = {
        'xsi_type': 'ExpandedTextAd',

        # Expanded text ads fields
        'headlinePart1': headline_part_1,
        'headlinePart2': headline_part_2,
        'description': description,
        'path1': path_1,
        'path2': path_2,
    }
    # we assume at this point that only one final url will be set
    if final_urls:
        # Specify a list of final URLs. This field cannot be set if URL
        # field is set, or finalUrls is unset. This may be specified at ad,
        # criterion, and feed item levels.
        ad['finalUrls'] = [final_urls]
    # we assume at this point that only one final url will be set
    if final_mobile_urls:
        # Specify a list of final URLs. This field cannot be set if URL
        # field is set, or finalUrls is unset. This may be specified at ad,
        # criterion, and feed item levels.
        ad['finalMobileUrls'] = [final_mobile_urls]
        # we assume at this point that only one final url will be set
    if final_app_urls:
        # Specify a list of final URLs. This field cannot be set if URL
        # field is set, or finalUrls is unset. This may be specified at ad,
        # criterion, and feed item levels.
        ad['finalAppUrls'] = [final_app_urls]
    if tracking_url_template:
        # Specify a tracking URL for 3rd party tracking provider. You may specify
        # one at customer, campaign, ad group, ad, criterion or feed item levels.
        ad['trackingUrlTemplate'] = tracking_url_template
    if url_customer_parameters:
        # Values for the parameters in the tracking URL. This can be provided at
        # campaign, ad group, ad, criterion, or feed item levels.
        parameters = []
        ad['urlCustomParameters'] = {'parameters': parameters}
        for k, v in url_customer_parameters.items():
            parameters.append({'key': k, 'value': v})
    return ad


def _abstract_ad(ad_id: 'Long' = None):
    ad = {
        'xsi_type': 'Ad',
        # Abstract ad for set and remove operations
        'id': ad_id,
    }
    return ad


def ad_label_operation(ad_id: 'Long' = None,
                       operator: 'String' = 'ADD',
                       ad_group_id: 'Long' = None,
                       label_id: 'Long' = None,
                       **kwargs):
    operation = {
        'operator': operator.upper(),
        'operand': {
            'adGroupId': ad_group_id,
            'adId': ad_id,
            'labelId': label_id

        }
    }
    return operation


def get_ad_operation(fields=[], predicates=[], **kwargs):
    default_fields = kwargs.pop('default_fields', False)
    if default_fields:
        fields = set(fields).union({'Id'})
    return _get_selector(fields, predicates)
