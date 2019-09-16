import freezegun

from django.db import connection, models

from psqlextra.auto_partition import (
    PostgresAutoPartitioningIntervalUnit,
    postgres_auto_partition,
)

from .fake_model import define_fake_partitioning_model


def test_auto_partition():
    """Tests whether automatically creating new partitions ahead works as
    expected."""

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

    with connection.cursor() as cursor:
        table = connection.introspection.get_partitioned_table(
            cursor, model._meta.db_table
        )

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

    with connection.cursor() as cursor:
        table = connection.introspection.get_partitioned_table(
            cursor, model._meta.db_table
        )

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

    with connection.cursor() as cursor:
        table = connection.introspection.get_partitioned_table(
            cursor, model._meta.db_table
        )

    assert len(table.partitions) == 14
    assert table.partitions[13].name == f"{model._meta.db_table}_1338_feb"
