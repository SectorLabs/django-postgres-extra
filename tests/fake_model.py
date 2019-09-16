import os
import sys
import uuid

from typing import List

from django.apps import AppConfig, apps
from django.db import connection
from django.db.models import Model

from psqlextra.models import PostgresModel, PostgresPartitionedModel


def define_fake_model(
    fields=None, model_base=PostgresModel, meta_options={}, **attributes
):
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


def define_fake_partitioning_model(
    fields=None, partitioning_options={}, meta_options={}
):
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
    """Creates a fake model to use during unit tests."""

    model = define_fake_model(fields, model_base, meta_options)

    with connection.schema_editor() as schema_editor:
        schema_editor.create_model(model)

    return model


def define_fake_app(models: List[Model] = []):
    name = str(uuid.uuid4()).replace("-", "")[:8] + "-app"

    app_config_cls = type(
        name + "Config",
        (AppConfig,),
        {"name": name, "path": os.path.dirname(__file__)},
    )

    app_config = app_config_cls(name, "")
    app_config.apps = apps

    app_config.models = {model.__name__: model for model in models}

    apps.app_configs[name] = app_config
    sys.modules[name] = {}

    return app_config
