import dj_database_url

DEBUG = True
TEMPLATE_DEBUG = True

SECRET_KEY = 'this is my secret key'  # NOQA

TEST_RUNNER = 'django.test.runner.DiscoverRunner'

DATABASES = {
    'default': dj_database_url.config(default='postgres:///postgres_extra')
}

DATABASES['default']['ENGINE'] = 'psqlextra.db'

LANGUAGE_CODE = 'en'
LANGUAGES = (
    ('en', 'English'),
    ('ro', 'Romanian'),
    ('nl', 'Dutch')
)

INSTALLED_APPS = (
    'tests',
)
