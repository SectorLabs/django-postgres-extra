from django.db.models.base import ModelBase

from .base import PostgresModel
from .options import PostgresViewOptions


class PostgresViewMeta(ModelBase):
    """Custom meta class for :see:PostgresView and
    :see:PostgresMaterializedView.

    This meta class extracts attributes from the inner
    `ViewMeta` class and copies it onto a `_vew_meta`
    attribute. This is similar to how Django's `_meta` works.
    """

    def __new__(cls, name, bases, attrs, **kwargs):
        new_class = super().__new__(cls, name, bases, attrs, **kwargs)
        meta_class = attrs.pop("ViewMeta", None)

        query = getattr(meta_class, "query", None)
        patitioning_meta = PostgresViewOptions(query=query)

        new_class.add_to_class("_view_meta", patitioning_meta)
        return new_class


class PostgresView(PostgresModel, metaclass=PostgresViewMeta):
    """Base class for creating a model that is a view."""

    class Meta:
        abstract = True
        base_manager_name = "objects"


class PostgresMaterializedView(PostgresModel, metaclass=PostgresViewMeta):
    """Base class for creating a model that is a materialized view."""

    class Meta:
        abstract = True
        base_manager_name = "objects"
