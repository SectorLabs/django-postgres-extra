from typing import Callable, Optional, Union

from django.core.exceptions import ImproperlyConfigured
from django.db import connections
from django.db.models import Model
from django.db.models.base import ModelBase
from django.db.models.query import QuerySet

from psqlextra.type_assertions import is_query_set, is_sql, is_sql_with_params
from psqlextra.types import SQL, SQLWithParams

from .base import PostgresModel
from .options import PostgresViewOptions

ViewQueryValue = Union[QuerySet, SQLWithParams, SQL]
ViewQuery = Optional[Union[ViewQueryValue, Callable[[], ViewQueryValue]]]


class PostgresViewModelMeta(ModelBase):
    """Custom meta class for :see:PostgresView and
    :see:PostgresMaterializedView.

    This meta class extracts attributes from the inner
    `ViewMeta` class and copies it onto a `_vew_meta`
    attribute. This is similar to how Django's `_meta` works.
    """

    def __new__(cls, name, bases, attrs, **kwargs):
        new_class = super().__new__(cls, name, bases, attrs, **kwargs)
        meta_class = attrs.pop("ViewMeta", None)

        view_query = getattr(meta_class, "query", None)
        sql_with_params = cls._view_query_as_sql_with_params(
            new_class, view_query
        )

        view_meta = PostgresViewOptions(query=sql_with_params)
        new_class.add_to_class("_view_meta", view_meta)
        return new_class

    @staticmethod
    def _view_query_as_sql_with_params(
        model: Model, view_query: ViewQuery
    ) -> Optional[SQLWithParams]:
        """Gets the query associated with the view as a raw SQL query with bind
        parameters.

        The query can be specified as a query set, raw SQL with params
        or without params. The query can also be specified as a callable
        which returns any of the above.

        When copying the meta options from the model, we convert any
        from the above to a raw SQL query with bind parameters. We do
        this is because it is what the SQL driver understands and
        we can easily serialize it into a migration.
        """

        # might be a callable to support delayed imports
        view_query = view_query() if callable(view_query) else view_query

        # make sure we don't do a boolean check on query sets,
        # because that might evaluate the query set
        if not is_query_set(view_query) and not view_query:
            return None

        is_valid_view_query = (
            is_query_set(view_query)
            or is_sql_with_params(view_query)
            or is_sql(view_query)
        )

        if not is_valid_view_query:
            raise ImproperlyConfigured(
                (
                    "Model '%s' is not properly configured to be a view."
                    " Set the `query` attribute on the `ViewMeta` class"
                    " to be a valid `django.db.models.query.QuerySet`"
                    " SQL string, or tuple of SQL string and params."
                )
                % (model.__name__)
            )

        # querysets can easily be converted into sql, params
        if is_query_set(view_query):
            return view_query.query.sql_with_params()

        # query was already specified in the target format
        if is_sql_with_params(view_query):
            return view_query

        return view_query, tuple()


class PostgresViewModel(PostgresModel, metaclass=PostgresViewModelMeta):
    """Base class for creating a model that is a view."""

    class Meta:
        abstract = True
        base_manager_name = "objects"


class PostgresMaterializedViewModel(
    PostgresViewModel, metaclass=PostgresViewModelMeta
):
    """Base class for creating a model that is a materialized view."""

    class Meta:
        abstract = True
        base_manager_name = "objects"

    @classmethod
    def refresh(
        cls, concurrently: bool = False, using: Optional[str] = None
    ) -> None:
        """Refreshes this materialized view.

        Arguments:
            concurrently:
                Whether to tell PostgreSQL to refresh this
                materialized view concurrently.

            using:
                Optionally, the name of the database connection
                to use for refreshing the materialized view.
        """

        conn_name = using or "default"

        with connections[conn_name].schema_editor() as schema_editor:
            schema_editor.refresh_materialized_view_model(cls, concurrently)
