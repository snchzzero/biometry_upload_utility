"""Const for biometry utility"""

# Definition names fields from m7_people service
M7_PEOPLE_ID = 'person_id'
M7_PEOPLE_NAME = 'first_name'
M7_PEOPLE_LAST_NAME = 'last_name'
M7_PEOPLE_PATRONYMIC = 'patronymic'

SERVICES_URL = {
    'm7_accounts': '{}://accounts.{}/jsonrpc/auth/v2?eaccess2=',
    'm7_people': '{}://{}/people/jsonrpc/people',
    'm7_biometry': '{}://biometry.{}/jsonrpc/biometry/v1'
}
