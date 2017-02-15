import dj_database_url

DEBUG = True
TEMPLATE_DEBUG = True

SECRET_KEY = 'this is my secret key'  # NOQA

TEST_RUNNER = 'django.test.runner.DiscoverRunner'

DATABASES = {
    'default': dj_database_url.config(default='postgres:///postgres_extra')
}

DATABASES['default']['ENGINE'] = 'postgres_extra.db'

LANGUAGE_CODE = 'en'
LANGUAGES = (
    ('en', 'English'),
    ('ro', 'Romanian'),
    ('nl', 'Dutch')
)

INSTALLED_APPS = (
    'tests',
)

# set to a lower number than the default, since
# we want the tests to be fast, default is 100
LOCALIZED_FIELDS_MAX_RETRIES = 3
