from .fields import HStoreField
from .materialized_view import PostgresMaterializedView

__all__ = [
    'HStoreField',
    'PostgresMaterializedView',
]

default_app_config = 'psqlextra.apps.PostgresExtraAppConfig'
