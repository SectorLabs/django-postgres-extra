from django import VERSION as DJANGO_VERSION


if DJANGO_VERSION < (3, 2):
    default_app_config = "psqlextra.apps.PostgresExtraAppConfig"
