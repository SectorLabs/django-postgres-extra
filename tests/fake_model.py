import os
import sys
import uuid

from contextlib import contextmanager

from django.apps import AppConfig, apps
from django.db import connection

from psqlextra.models import (
    PostgresMaterializedViewModel,
    PostgresModel,
    PostgresPartitionedModel,
    PostgresViewModel,
)


def define_fake_model(
    fields=None, model_base=PostgresModel, meta_options={}, **attributes
):
    """Defines a fake model (but does not create it in the database)."""

    name = str(uuid.uuid4()).replace("-", "")[:8].title()

    attributes = {
        "app_label": meta_options.get("app_label") or "tests",
        "__module__": __name__,
        "__name__": name,
        "Meta": type("Meta", (object,), meta_options),
        **attributes,
    }

    if fields:
        attributes.update(fields)

    model = type(name, (model_base,), attributes)

    apps.app_configs[attributes["app_label"]].models[name] = model
    return model


def define_fake_view_model(
    fields=None, view_options={}, meta_options={}, model_base=PostgresViewModel
):
    """Defines a fake view model."""

    model = define_fake_model(
        fields=fields,
        model_base=model_base,
        meta_options=meta_options,
        ViewMeta=type("ViewMeta", (object,), view_options),
    )

    return model


def define_fake_materialized_view_model(
    fields=None,
    view_options={},
    meta_options={},
    model_base=PostgresMaterializedViewModel,
):
    """Defines a fake materialized view model."""

    model = define_fake_model(
        fields=fields,
        model_base=model_base,
        meta_options=meta_options,
        ViewMeta=type("ViewMeta", (object,), view_options),
    )

    return model


def define_fake_partitioned_model(
    fields=None, partitioning_options={}, meta_options={}
):
    """Defines a fake partitioned model."""

    model = define_fake_model(
        fields=fields,
        model_base=PostgresPartitionedModel,
        meta_options=meta_options,
        PartitioningMeta=type(
            "PartitioningMeta", (object,), partitioning_options
        ),
    )

    return model


def get_fake_model(fields=None, model_base=PostgresModel, meta_options={}):
    """Defines a fake model and creates it in the database."""

    model = define_fake_model(fields, model_base, meta_options)

    with connection.schema_editor() as schema_editor:
        schema_editor.create_model(model)

    return model


@contextmanager
def define_fake_app():
    """Creates and registers a fake Django app."""

    name = str(uuid.uuid4()).replace("-", "")[:8] + "-app"

    app_config_cls = type(
        name + "Config",
        (AppConfig,),
        {"name": name, "path": os.path.dirname(__file__)},
    )

    app_config = app_config_cls(name, "")
    app_config.apps = apps
    app_config.models = {}

    apps.app_configs[name] = app_config
    sys.modules[name] = {}

    try:
        yield app_config
    finally:
        del apps.app_configs[name]
        del sys.modules[name]
