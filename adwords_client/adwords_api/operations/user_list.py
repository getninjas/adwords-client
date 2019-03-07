from .utils import _get_selector
import hashlib


def user_list_operation(user_list_id: 'Long' = None,
                         name: 'String' = None,
                         description: 'String' = None,
                         status: 'String' = 'OPEN',
                         operator: 'String' = 'ADD',
                         is_eligible_for_search: 'Boolean' = True,
                         is_eligible_for_display: 'Boolean' = True,
                         userlist_type: 'String' = 'CRM_BASED',
                         integration_code: 'String' = None,
                         app_id: 'String' = None,
                         upload_key_type: 'String' = 'CONTACT_INFO',
                         membership_life_span: 'Long' = 10000,
                         data_source_type: 'String' = 'FIRST_PARTY',
                         **kwargs):

    list_types = {
        'CRM_BASED': 'CrmBasedUserList',
        'BASIC': 'BasicUserList',
        'LOGICAL': 'LogicalUserList',
        'RULE_BASED': 'RuledBasedUserList',
        'SIMILAR': 'SimilarUserList',
    }

    if userlist_type != 'CRM_BASED':
        raise NotImplementedError(userlist_type)

    operation = {
        'xsi_type': 'UserListOperation',
        'operator': operator.upper(),
        'operand': {
            'xsi_type': list_types[userlist_type],
            'status': status,
            'isEligibleForSearch': is_eligible_for_search,
            'isEligibleForDisplay': is_eligible_for_display,
            'membershipLifeSpan': membership_life_span,
        }
    }

    if user_list_id:
        operation['operand']['id'] = user_list_id
    if name:
        operation['operand']['name'] = name
    if description:
        operation['operand']['description'] = description
    if integration_code:
        operation['operand']['integrationCode'] = integration_code
    if userlist_type == 'CRM_BASED':
        operation['operand']['uploadKeyType'] = upload_key_type
        operation['operand']['dataSourceType'] = data_source_type
        if app_id:
            operation['operand']['appId'] = app_id

    return operation


def list_members_operation(user_list_id: 'Long' = None,
                           operator: 'String' = 'ADD',
                           remove_all : 'Boolean' = False,
                           members: 'List' = [],
                           **kwargs
                           ):
    operation = {
        'xsi_type': 'MutateMembersOperation',
        'operator': operator.upper(),
        'operand': {
            'xsi_type': 'MutateMembersOperand',
            'userListId': user_list_id,
            'removeAll': remove_all,
            'membersList': []
        }
    }

    for member in members:
        member_data = {}
        if member.get('email', None):
            member_data['hashedEmail'] = normalize_and_sha256(member['email'])
        if member.get('mobile_id', None):
            member_data['mobileId'] = member['mobile_id']
        if member.get('phone_number', None):
            member_data['hashedPhoneNumber'] = normalize_and_sha256(member['phone_number'])

        address_fields = ['first_name', 'last_name', 'country_code', 'zip_code']
        if all(bool(member.get(field, None)) for field in address_fields):
            member_data['addressInfo'] = {}
            member_data['addressInfo']['hashedFirstName'] = normalize_and_sha256(member['first_name'])
            member_data['addressInfo']['hashedLastName'] = normalize_and_sha256(member['last_name'])
            member_data['addressInfo']['countryCode'] = member['country_code']
            member_data['addressInfo']['zipCode'] = member['zip_code']
        operation['operand']['membersList'].append(member_data)
    return operation


def normalize_and_sha256(s):
    return hashlib.sha256(s.strip().lower().encode()).hexdigest()


def query_members(query: 'String' = ''):
    return query


def get_user_list_operation(fields=[], predicates=[], **kwargs):
    default_fields = kwargs.pop('default_fields', False)
    if default_fields:
        fields = set(fields).union({'Id', 'Name'})
    return _get_selector(fields, predicates)
