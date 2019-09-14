import uuid

from django.db import connection

from psqlextra.models import PostgresModel, PostgresPartitionedModel


def define_fake_model(
    fields=None, model_base=PostgresModel, meta_options={}, **attributes
):
    name = str(uuid.uuid4()).replace("-", "")[:8]

    attributes = {
        "app_label": "tests",
        "__module__": __name__,
        "__name__": name,
        "Meta": type("Meta", (object,), meta_options),
        **attributes,
    }

    if fields:
        attributes.update(fields)

    model = type(name, (model_base,), attributes)
    return model


def define_fake_partitioning_model(fields=None, partitioning_options={}):
    return define_fake_model(
        fields=fields,
        model_base=PostgresPartitionedModel,
        meta_options={},
        PartitioningMeta=type(
            "PartitioningMeta", (object,), partitioning_options
        ),
    )


def get_fake_model(fields=None, model_base=PostgresModel, meta_options={}):
    """Creates a fake model to use during unit tests."""

    model = define_fake_model(fields, model_base, meta_options)

    with connection.schema_editor() as schema_editor:
        schema_editor.create_model(model)

    return model
