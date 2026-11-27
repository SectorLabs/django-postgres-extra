import django
import pytest

from django.apps import apps
from django.db import models
from django.db.migrations import AddField, AlterField, RemoveField
from django.db.migrations.state import ProjectState

from psqlextra.backend.migrations import operations, postgres_patched_migrations
from psqlextra.models import (
    PostgresMaterializedViewModel,
    PostgresPartitionedModel,
    PostgresViewModel,
)
from psqlextra.types import PostgresPartitioningMethod

from .fake_model import (
    define_fake_materialized_view_model,
    define_fake_model,
    define_fake_partitioned_model,
    define_fake_view_model,
    get_fake_model,
)
from .migrations import apply_migration, make_migration


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
        dict(
            fields={"artist_id": models.IntegerField()},
            partitioning_options=dict(
                method=PostgresPartitioningMethod.HASH, key="artist_id"
            ),
        ),
        dict(
            fields={"category": models.IntegerField(), "timestamp": models.DateTimeField()},
            partitioning_options=dict(
                method=PostgresPartitioningMethod.LIST, key="category",
                sub_key=["timestamp"], sub_method=PostgresPartitioningMethod.RANGE
            ),
        ),
    ],
)
@postgres_patched_migrations()
def test_make_migration_create_partitioned_model(fake_app, model_config):
    """Tests whether the right operations are generated when creating a new
    partitioned model."""

    model = define_fake_partitioned_model(
        **model_config, meta_options=dict(app_label=fake_app.name)
    )

    migration = make_migration(fake_app.name)
    ops = migration.operations
    method = model_config["partitioning_options"]["method"]

    if method == PostgresPartitioningMethod.HASH:
        # should have one operation to create the partitioned model
        # and no default partition
        assert len(ops) == 1
        assert isinstance(ops[0], operations.PostgresCreatePartitionedModel)
    else:
        # should have one operation to create the partitioned model
        # and one more to add a default partition
        assert len(ops) == 2
        assert isinstance(ops[0], operations.PostgresCreatePartitionedModel)
        assert isinstance(ops[1], operations.PostgresAddDefaultPartition)

        # make sure the default partition is named "default"
        assert ops[1].model_name == model.__name__
        assert ops[1].name == "default"

    # make sure the base is set correctly
    assert len(ops[0].bases) == 1
    assert issubclass(ops[0].bases[0], PostgresPartitionedModel)

    # make sure the partitioning options got copied correctly (not considering None values)
    assert {k:v for k, v in ops[0].partitioning_options.items() if v is not None} == model_config["partitioning_options"]


@postgres_patched_migrations()
def test_make_migration_create_view_model(fake_app):
    """Tests whether the right operations are generated when creating a new
    view model."""

    underlying_model = get_fake_model({"name": models.TextField()})

    model = define_fake_view_model(
        fields={"name": models.TextField()},
        view_options=dict(query=underlying_model.objects.all()),
        meta_options=dict(app_label=fake_app.name),
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


@postgres_patched_migrations()
def test_make_migration_create_materialized_view_model(fake_app):
    """Tests whether the right operations are generated when creating a new
    materialized view model."""

    underlying_model = get_fake_model({"name": models.TextField()})

    model = define_fake_materialized_view_model(
        fields={"name": models.TextField()},
        view_options=dict(query=underlying_model.objects.all()),
        meta_options=dict(app_label=fake_app.name),
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


@pytest.mark.parametrize(
    "define_view_model",
    [define_fake_materialized_view_model, define_fake_view_model],
)
@postgres_patched_migrations()
def test_make_migration_field_operations_view_models(
    fake_app, define_view_model
):
    """Tests whether field operations against a (materialized) view are always
    wrapped in the :see:ApplyState operation so that they don't actually get
    applied to the database, yet Django applies to them to the project state.

    This is important because you can't actually alter/add or delete
    fields from a (materialized) view.
    """

    underlying_model = get_fake_model(
        {"first_name": models.TextField(), "last_name": models.TextField()},
        meta_options=dict(app_label=fake_app.name),
    )

    model = define_view_model(
        fields={"first_name": models.TextField()},
        view_options=dict(query=underlying_model.objects.all()),
        meta_options=dict(app_label=fake_app.name),
    )

    state_1 = ProjectState.from_apps(apps)

    migration = make_migration(model._meta.app_label)
    apply_migration(migration.operations, state_1)

    # add a field to the materialized view
    last_name_field = models.TextField(null=True)
    last_name_field.contribute_to_class(model, "last_name")

    migration = make_migration(model._meta.app_label, from_state=state_1)
    assert len(migration.operations) == 1
    assert isinstance(migration.operations[0], operations.ApplyState)
    assert isinstance(migration.operations[0].state_operation, AddField)

    # alter the field on the materialized view
    state_2 = ProjectState.from_apps(apps)
    last_name_field = models.TextField(null=True, blank=True)
    last_name_field.contribute_to_class(model, "last_name")

    migration = make_migration(model._meta.app_label, from_state=state_2)
    assert len(migration.operations) == 1
    assert isinstance(migration.operations[0], operations.ApplyState)
    assert isinstance(migration.operations[0].state_operation, AlterField)

    # remove the field from the materialized view
    migration = make_migration(
        model._meta.app_label,
        from_state=ProjectState.from_apps(apps),
        to_state=state_1,
    )
    assert isinstance(migration.operations[0], operations.ApplyState)
    assert isinstance(migration.operations[0].state_operation, RemoveField)


@pytest.mark.skipif(
    django.VERSION < (2, 2),
    reason="Django < 2.2 doesn't implement left-to-right migration optimizations",
)
@pytest.mark.parametrize("method", PostgresPartitioningMethod.all())
@postgres_patched_migrations()
def test_autodetect_fk_issue(fake_app, method):
    """Test whether Django can perform ForeignKey optimization.

    Fixes
    https://github.com/SectorLabs/django-postgres-extra/issues/123
    for Django >= 2.2
    """
    meta_options = {"app_label": fake_app.name}
    partitioning_options = {"method": method, "key": "artist_id"}

    artist_model_fields = {"name": models.TextField()}
    Artist = define_fake_model(artist_model_fields, meta_options=meta_options)

    from_state = ProjectState.from_apps(apps)

    album_model_fields = {
        "name": models.TextField(),
        "artist": models.ForeignKey(
            to=Artist.__name__, on_delete=models.CASCADE
        ),
    }

    define_fake_partitioned_model(
        album_model_fields,
        partitioning_options=partitioning_options,
        meta_options=meta_options,
    )

    migration = make_migration(fake_app.name, from_state=from_state)
    ops = migration.operations

    if method == PostgresPartitioningMethod.HASH:
        assert len(ops) == 1
        assert isinstance(ops[0], operations.PostgresCreatePartitionedModel)
    else:
        assert len(ops) == 2
        assert isinstance(ops[0], operations.PostgresCreatePartitionedModel)
        assert isinstance(ops[1], operations.PostgresAddDefaultPartition)
