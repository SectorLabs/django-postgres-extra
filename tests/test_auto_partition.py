import freezegun

from django.db import connection, models

from psqlextra.auto_partition import (
    PostgresAutoPartitioningIntervalUnit,
    postgres_auto_partition,
)

from .fake_model import define_fake_partitioning_model


def _get_partitioned_table(model):
    with connection.cursor() as cursor:
        return connection.introspection.get_partitioned_table(
            cursor, model._meta.db_table
        )


def test_auto_partition_monthly():
    """Tests whether automatically creating new partitions ahead monthly works
    as expected."""

    model = define_fake_partitioning_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    # create partitions for the next 12 months (including the current)
    with freezegun.freeze_time("1337-01-30"):
        postgres_auto_partition(
            model,
            count=12,
            interval_unit=PostgresAutoPartitioningIntervalUnit.MONTH,
            interval=1,
        )

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 12
    assert table.partitions[0].name == f"{model._meta.db_table}_1337_jan"
    assert table.partitions[1].name == f"{model._meta.db_table}_1337_feb"
    assert table.partitions[2].name == f"{model._meta.db_table}_1337_mar"
    assert table.partitions[3].name == f"{model._meta.db_table}_1337_apr"
    assert table.partitions[4].name == f"{model._meta.db_table}_1337_may"
    assert table.partitions[5].name == f"{model._meta.db_table}_1337_jun"
    assert table.partitions[6].name == f"{model._meta.db_table}_1337_jul"
    assert table.partitions[7].name == f"{model._meta.db_table}_1337_aug"
    assert table.partitions[8].name == f"{model._meta.db_table}_1337_sep"
    assert table.partitions[9].name == f"{model._meta.db_table}_1337_oct"
    assert table.partitions[10].name == f"{model._meta.db_table}_1337_nov"
    assert table.partitions[11].name == f"{model._meta.db_table}_1337_dec"

    # re-running it with 13, should just create one additional partition
    with freezegun.freeze_time("1337-01-30"):
        postgres_auto_partition(
            model,
            count=13,
            interval_unit=PostgresAutoPartitioningIntervalUnit.MONTH,
            interval=1,
        )

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 13
    assert table.partitions[12].name == f"{model._meta.db_table}_1338_jan"

    # it's november now, we only want to create 4 partitions ahead,
    # so only one new partition should be created for february 1338
    with freezegun.freeze_time("1337-11-01"):
        postgres_auto_partition(
            model,
            count=4,
            interval_unit=PostgresAutoPartitioningIntervalUnit.MONTH,
            interval=1,
        )

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 14
    assert table.partitions[13].name == f"{model._meta.db_table}_1338_feb"


def test_auto_partition_weekly():
    """Tests whether automatically creating new partitions ahead weekly works
    as expected."""

    model = define_fake_partitioning_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    # create partitions for the next 4 weeks (including the current)
    with freezegun.freeze_time("1337-01-23"):
        postgres_auto_partition(
            model,
            count=4,
            interval_unit=PostgresAutoPartitioningIntervalUnit.WEEK,
            interval=1,
        )

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 4
    assert table.partitions[0].name == f"{model._meta.db_table}_1337_week_03"
    assert table.partitions[1].name == f"{model._meta.db_table}_1337_week_04"
    assert table.partitions[2].name == f"{model._meta.db_table}_1337_week_05"
    assert table.partitions[3].name == f"{model._meta.db_table}_1337_week_06"

    # re-running it with 5, should just create one additional partition
    with freezegun.freeze_time("1337-01-23"):
        postgres_auto_partition(
            model,
            count=5,
            interval_unit=PostgresAutoPartitioningIntervalUnit.WEEK,
            interval=1,
        )

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 5
    assert table.partitions[4].name == f"{model._meta.db_table}_1337_week_07"

    # it's june now, we want to partition two weeks ahead
    with freezegun.freeze_time("1337-06-03"):
        postgres_auto_partition(
            model,
            count=2,
            interval_unit=PostgresAutoPartitioningIntervalUnit.WEEK,
            interval=1,
        )

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 7
    assert table.partitions[5].name == f"{model._meta.db_table}_1337_week_22"
    assert table.partitions[6].name == f"{model._meta.db_table}_1337_week_23"