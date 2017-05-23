from .materialized_view import PostgresMaterializedView

__all__ = [
    'PostgresMaterializedView',
]

default_app_config = 'psqlextra.apps.PostgresExtraAppConfig'
