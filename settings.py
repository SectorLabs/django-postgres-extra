import os

from urllib.parse import urlparse


def _parse_db_url(url: str):
    parsed_url = urlparse(url)

    return {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': (parsed_url.path or '').strip('/') or "postgres",
        'HOST': parsed_url.hostname or None,
        'PORT': parsed_url.port or None,
        'USER': parsed_url.username or None,
        'PASSWORD': parsed_url.password or None,
    }


DEBUG = True
TEMPLATE_DEBUG = True

SECRET_KEY = 'this is my secret key'  # NOQA

TEST_RUNNER = 'django.test.runner.DiscoverRunner'

DATABASES = {
    'default': _parse_db_url(os.environ.get('DATABASE_URL', 'postgres:///psqlextra')),
}

DATABASES['default']['ENGINE'] = 'tests.psqlextra_test_backend'

LANGUAGE_CODE = 'en'
LANGUAGES = (
    ('en', 'English'),
    ('ro', 'Romanian'),
    ('nl', 'Dutch')
)

INSTALLED_APPS = (
    'psqlextra',
    'tests',
)

USE_TZ = True
TIME_ZONE = 'UTC'

DATABASE_IN_CONTAINER = os.environ.get('DATABASE_IN_CONTAINER') == 'true'
