import django

from ._version import __version__

if django.VERSION < (3, 2):  # pragma: no cover
    default_app_config = "psqlextra.apps.PostgresExtraAppConfig"

    __all__ = [
        "default_app_config",
        "__version__",
    ]
else:
    __all__ = [
        "__version__",
    ]
