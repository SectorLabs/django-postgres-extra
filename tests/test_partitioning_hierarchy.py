
import django
import freezegun
import pytest
from dateutil.relativedelta import relativedelta
from django.core.exceptions import ImproperlyConfigured
from django.db import connection, models
from psqlextra.backend.schema import PostgresSchemaEditor
from psqlextra.contrib import partition_by_category_and_current_time
from psqlextra.partitioning import (
    PostgresPartitioningManager,
)
from psqlextra.types import PostgresPartitioningMethod

from . import db_introspection
from .fake_model import define_fake_partitioned_model


def _get_partitioned_table(model):
    return db_introspection.get_partitioned_table(model._meta.db_table)

def _get_sub_partitions(model,partition_name):
    return db_introspection.get_partitioned_table(model._meta.db_table + "_" + partition_name)

@pytest.mark.skipif(
    django.VERSION < (5, 2),
    reason="Django < 5.2 doesn't implement composite primary keys",
)
@pytest.mark.postgres_version(lt=110000)
def test_partitioning_hierarchy_time_yearly_apply():
    """Tests whether automatically creating new partitions ahead yearly works
    as expected."""

    model = define_fake_partitioned_model(
        fields={
            "id": models.AutoField(primary_key=False),
            "category_id": models.IntegerField(),
            "date": models.DateTimeField(),
            "my_custom_pk": models.CompositePrimaryKey("id", "category_id", "date"),
        },
        partitioning_options=dict(
            key=["category_id"],
            method=PostgresPartitioningMethod.LIST,
            sub_key=["date"],
            sub_method=PostgresPartitioningMethod.RANGE
            )
    )

    # pylint: disable=protected-access
    # pylint: disable=no-member
    assert isinstance(model._meta.pk, models.CompositePrimaryKey)
    assert model._meta.pk.name == "my_custom_pk"
    assert model._meta.pk.columns == ("id", "category_id", "date")
    # pylint: enable=protected-access
    # pylint: enable=no-member

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    with freezegun.freeze_time("2019-1-1"):
        manager = PostgresPartitioningManager(
            [partition_by_category_and_current_time(model, categories=[1, 2], years=1, count=2)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 2
    assert table.partitions[0].name == "1"
    assert table.partitions[1].name == "2"

    sub_table = _get_sub_partitions(model, table.partitions[0].name)
    assert len(sub_table.partitions) == 2
    assert sub_table.partitions[0].full_name.endswith("1_2019")
    assert sub_table.partitions[1].full_name.endswith("1_2020")

    sub_table = _get_sub_partitions(model, table.partitions[1].name)
    assert len(sub_table.partitions) == 2
    assert sub_table.partitions[0].full_name.endswith("2_2019")
    assert sub_table.partitions[1].full_name.endswith("2_2020")



@pytest.mark.postgres_version(lt=110000)
@pytest.mark.parametrize(
    "kwargs,timepoints",
    [
        (
            dict(years=1, max_age=relativedelta(years=2)),
            [("2019-1-1", 6), ("2020-1-1", 6), ("2021-1-1", 5)],
        ),
        (
            dict(months=1, max_age=relativedelta(months=1)),
            [
                ("2019-1-1", 6),
                ("2019-2-1", 5),
                ("2019-2-28", 5),
                ("2019-3-1", 4),
            ],
        ),
        (
            dict(days=7, max_age=relativedelta(weeks=1)),
            [
                ("2019-1-1", 6),
                ("2019-1-4", 5),
                ("2019-1-8", 5),
                ("2019-1-15", 4),
                ("2019-1-16", 4),
            ],
        ),
    ],
)
def test_partitioning_hierarchy_time_delete(kwargs, timepoints):
    """Tests whether partitions older than the specified max_age are
    automatically deleted."""

    model = define_fake_partitioned_model(
        fields={
            "id": models.AutoField(primary_key=False),
            "category_id": models.IntegerField(),
            "date": models.DateTimeField(),
            "my_custom_pk": models.CompositePrimaryKey("id", "category_id", "date"),
        },
        partitioning_options=dict(
            key=["category_id"],
            method=PostgresPartitioningMethod.LIST,
            sub_key=["date"],
            sub_method=PostgresPartitioningMethod.RANGE
            )
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    partition_kwargs = {"model": model, "categories": [1, 2], "count": 6, **kwargs}

    print("test with kwargs:", partition_kwargs)
    manager = PostgresPartitioningManager(
        [partition_by_category_and_current_time(**partition_kwargs)]
    )

    with freezegun.freeze_time(timepoints[0][0]):
        manager.plan().apply()

    for (dt, partition_count) in timepoints:
        with freezegun.freeze_time(dt):
            manager.plan(skip_create=True).apply()

            table = _get_partitioned_table(model)
            sub_table = _get_sub_partitions(model, table.partitions[0].name)
            assert len(sub_table.partitions) == partition_count

            sub_table = _get_sub_partitions(model, table.partitions[1].name)
            assert len(sub_table.partitions) == partition_count


def test_schema_editor_create_sub_partitioned_model_no_subkey():
    """Tests whether trying to create a partitioned model without a
    partitioning key raises :see:ImproperlyConfigured as its not possible to
    create a partitioned model without one and we cannot have a sane
    default."""

    model = define_fake_partitioned_model(
        fields={
            "id": models.AutoField(primary_key=False),
            "category_id": models.IntegerField(),
            "date": models.DateTimeField(),
            "my_custom_pk": models.CompositePrimaryKey("id", "category_id", "date"),
        },
        partitioning_options=dict(
            key=["category_id"],
            method=PostgresPartitioningMethod.LIST,
            sub_method=PostgresPartitioningMethod.RANGE
            )
    )

    schema_editor = PostgresSchemaEditor(connection)

    with pytest.raises(ImproperlyConfigured):
        schema_editor.create_partitioned_model(model)


def test_schema_editor_create_sub_partitioned_model_no_field():
    """Tests whether trying to create a partitioned model without a
    partitioning key raises :see:ImproperlyConfigured as its not possible to
    create a partitioned model without one and we cannot have a sane
    default."""

    model = define_fake_partitioned_model(
        fields={
            "id": models.AutoField(primary_key=False),
            "category_id": models.IntegerField(),
            "my_custom_pk": models.CompositePrimaryKey("id", "category_id", "date"),
        },
        partitioning_options=dict(
            key=["category_id"],
            method=PostgresPartitioningMethod.LIST,
            sub_method=PostgresPartitioningMethod.RANGE,
            sub_key=["date"]
            )
    )

    schema_editor = PostgresSchemaEditor(connection)

    with pytest.raises(ImproperlyConfigured):
        schema_editor.create_partitioned_model(model)



@pytest.mark.skipif(
    django.VERSION < (5, 2),
    reason="Django < 5.2 doesn't implement composite primary keys",
)
@pytest.mark.postgres_version(lt=110000)
def test_partitioning_hierarchy_custom_name():
    """Tests whether custom name partitions work as expected."""

    model = define_fake_partitioned_model(
        fields={
            "id": models.AutoField(primary_key=False),
            "category_id": models.IntegerField(),
            "date": models.DateTimeField(),
            "my_custom_pk": models.CompositePrimaryKey("id", "category_id", "date"),
        },
        partitioning_options=dict(
            key=["category_id"],
            method=PostgresPartitioningMethod.LIST,
            sub_key=["date"],
            sub_method=PostgresPartitioningMethod.RANGE,
            )
    )




    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    with freezegun.freeze_time("2019-1-1"):
        manager = PostgresPartitioningManager(
            [partition_by_category_and_current_time(
                model,
                categories=[1, 2],
                years=1,
                count=2,
                name_format=("category_%s", "time_%Y"),
            )]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 2
    assert table.partitions[0].name == "category_1"
    assert table.partitions[1].name == "category_2"

    sub_table = _get_sub_partitions(model, table.partitions[0].name)
    assert len(sub_table.partitions) == 2
    assert sub_table.partitions[0].full_name.endswith("category_1_time_2019")
    assert sub_table.partitions[1].full_name.endswith("category_1_time_2020")

    sub_table = _get_sub_partitions(model, table.partitions[1].name)
    assert len(sub_table.partitions) == 2
    assert sub_table.partitions[0].full_name.endswith("category_2_time_2019")
    assert sub_table.partitions[1].full_name.endswith("category_2_time_2020")
