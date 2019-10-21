import pytest

from django.db import models

from psqlextra.backend.migrations import operations
from psqlextra.models import (
    PostgresMaterializedViewModel,
    PostgresPartitionedModel,
    PostgresViewModel,
)
from psqlextra.types import PostgresPartitioningMethod

from .fake_model import (
    define_fake_app,
    define_fake_materialized_view_model,
    define_fake_partitioned_model,
    define_fake_view_model,
    get_fake_model,
)
from .migrations import make_migration


@pytest.mark.parametrize(
    "model_config",
    [
        dict(
            fields={"category": models.TextField()},
            partitioning_options=dict(
                method=PostgresPartitioningMethod.LIST, key="category"
            ),
        ),
        dict(
            fields={"timestamp": models.DateTimeField()},
            partitioning_options=dict(
                method=PostgresPartitioningMethod.RANGE, key="timestamp"
            ),
        ),
    ],
)
def test_make_migration_create_partitioned_model(model_config):
    """Tests whether the right operations are generated when creating a new
    partitioned model."""

    app_config = define_fake_app()

    model = define_fake_partitioned_model(
        **model_config, meta_options=dict(app_label=app_config.name)
    )

    migration = make_migration(model._meta.app_label)
    ops = migration.operations

    # should have one operation to create the partitioned model
    # and one more to add a default partition
    assert len(ops) == 2
    assert isinstance(ops[0], operations.PostgresCreatePartitionedModel)
    assert isinstance(ops[1], operations.PostgresAddDefaultPartition)

    # make sure the base is set correctly
    assert len(ops[0].bases) == 1
    assert issubclass(ops[0].bases[0], PostgresPartitionedModel)

    # make sure the partitioning options got copied correctly
    assert ops[0].partitioning_options == model_config["partitioning_options"]

    # make sure the default partition is named "default"
    assert ops[1].model_name == model.__name__
    assert ops[1].name == "default"


def test_make_migration_create_view_model():
    """Tests whether the right operations are generated when creating a new
    view model."""

    underlying_model = get_fake_model({"name": models.TextField()})

    app_config = define_fake_app()

    model = define_fake_view_model(
        fields={"timestamp": models.DateTimeField()},
        view_options=dict(query=underlying_model.objects.all()),
        meta_options=dict(app_label=app_config.name),
    )

    migration = make_migration(model._meta.app_label)
    ops = migration.operations

    assert len(ops) == 1
    assert isinstance(ops[0], operations.PostgresCreateViewModel)

    # make sure the base is set correctly
    assert len(ops[0].bases) == 1
    assert issubclass(ops[0].bases[0], PostgresViewModel)

    # make sure the view options got copied correctly
    assert ops[0].view_options == model._view_meta.original_attrs


def test_make_migration_create_materialized_view_model():
    """Tests whether the right operations are generated when creating a new
    materialized view model."""

    underlying_model = get_fake_model({"name": models.TextField()})

    app_config = define_fake_app()

    model = define_fake_materialized_view_model(
        fields={"timestamp": models.DateTimeField()},
        view_options=dict(query=underlying_model.objects.all()),
        meta_options=dict(app_label=app_config.name),
    )

    migration = make_migration(model._meta.app_label)
    ops = migration.operations

    assert len(ops) == 1
    assert isinstance(ops[0], operations.PostgresCreateMaterializedViewModel)

    # make sure the base is set correctly
    assert len(ops[0].bases) == 1
    assert issubclass(ops[0].bases[0], PostgresMaterializedViewModel)

    # make sure the view options got copied correctly
    assert ops[0].view_options == model._view_meta.original_attrs
