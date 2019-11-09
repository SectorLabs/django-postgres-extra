import datetime

import freezegun
import pytest

from django.db import connection, models, transaction
from django.db.utils import IntegrityError, ProgrammingError

from psqlextra.partitioning import (
    PostgresPartitioningError,
    PostgresPartitioningManager,
    partition_by_time,
)

from . import db_introspection
from .fake_model import define_fake_partitioned_model


def _get_partitioned_table(model):
    return db_introspection.get_partitioned_table(model._meta.db_table)


def test_partitioning_time_yearly_auto_create():
    """Tests whether automatically creating new partitions ahead yearly works
    as expected."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    with freezegun.freeze_time("1337-01-01"):
        manager = PostgresPartitioningManager(
            [partition_by_time(model, years=1, count=2)]
        )
        manager.auto_create()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 2
    assert table.partitions[0].name == "1337"
    assert table.partitions[1].name == "1338"

    with freezegun.freeze_time("1337-12-30"):
        manager = PostgresPartitioningManager(
            [partition_by_time(model, years=1, count=3)]
        )
        manager.auto_create()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 3
    assert table.partitions[0].name == "1337"
    assert table.partitions[1].name == "1338"
    assert table.partitions[2].name == "1339"


def test_partitioning_time_monthly_auto_create():
    """Tests whether automatically creating new partitions ahead monthly works
    as expected."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    # create partitions for the next 12 months (including the current)
    with freezegun.freeze_time("1337-01-30"):
        manager = PostgresPartitioningManager(
            [partition_by_time(model, months=1, count=12)]
        )
        manager.auto_create()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 12
    assert table.partitions[0].name == "1337_jan"
    assert table.partitions[1].name == "1337_feb"
    assert table.partitions[2].name == "1337_mar"
    assert table.partitions[3].name == "1337_apr"
    assert table.partitions[4].name == "1337_may"
    assert table.partitions[5].name == "1337_jun"
    assert table.partitions[6].name == "1337_jul"
    assert table.partitions[7].name == "1337_aug"
    assert table.partitions[8].name == "1337_sep"
    assert table.partitions[9].name == "1337_oct"
    assert table.partitions[10].name == "1337_nov"
    assert table.partitions[11].name == "1337_dec"

    # re-running it with 13, should just create one additional partition
    with freezegun.freeze_time("1337-01-30"):
        manager = PostgresPartitioningManager(
            [partition_by_time(model, months=1, count=13)]
        )
        manager.auto_create()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 13
    assert table.partitions[12].name == "1338_jan"

    # it's november now, we only want to create 4 partitions ahead,
    # so only one new partition should be created for february 1338
    with freezegun.freeze_time("1337-11-01"):
        manager = PostgresPartitioningManager(
            [partition_by_time(model, months=1, count=4)]
        )
        manager.auto_create()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 14
    assert table.partitions[13].name == "1338_feb"


def test_partitioning_time_weekly_auto_create():
    """Tests whether automatically creating new partitions ahead weekly works
    as expected."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    # create partitions for the next 4 weeks (including the current)
    with freezegun.freeze_time("1337-01-23"):
        manager = PostgresPartitioningManager(
            [partition_by_time(model, weeks=1, count=4)]
        )
        manager.auto_create()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 4
    assert table.partitions[0].name == "1337_week_03"
    assert table.partitions[1].name == "1337_week_04"
    assert table.partitions[2].name == "1337_week_05"
    assert table.partitions[3].name == "1337_week_06"

    # re-running it with 5, should just create one additional partition
    with freezegun.freeze_time("1337-01-23"):
        manager = PostgresPartitioningManager(
            [partition_by_time(model, weeks=1, count=5)]
        )
        manager.auto_create()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 5
    assert table.partitions[4].name == "1337_week_07"

    # it's june now, we want to partition two weeks ahead
    with freezegun.freeze_time("1337-06-03"):
        manager = PostgresPartitioningManager(
            [partition_by_time(model, weeks=1, count=2)]
        )
        manager.auto_create()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 7
    assert table.partitions[5].name == "1337_week_22"
    assert table.partitions[6].name == "1337_week_23"


def test_partitioning_time_daily_auto_create():
    """Tests whether automatically creating new partitions ahead daily works as
    expected."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    # create partitions for the next 4 days (including the current)
    with freezegun.freeze_time("1337-01-23"):
        manager = PostgresPartitioningManager(
            [partition_by_time(model, days=1, count=4)]
        )
        manager.auto_create()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 4
    assert table.partitions[0].name == "1337_jan_23"
    assert table.partitions[1].name == "1337_jan_24"
    assert table.partitions[2].name == "1337_jan_25"
    assert table.partitions[3].name == "1337_jan_26"

    # re-running it with 5, should just create one additional partition
    with freezegun.freeze_time("1337-01-23"):
        manager = PostgresPartitioningManager(
            [partition_by_time(model, days=1, count=5)]
        )
        manager.auto_create()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 5
    assert table.partitions[4].name == "1337_jan_27"

    # it's june now, we want to partition two days ahead
    with freezegun.freeze_time("1337-06-03"):
        manager = PostgresPartitioningManager(
            [partition_by_time(model, days=1, count=2)]
        )
        manager.auto_create()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 7
    assert table.partitions[5].name == "1337_jun_03"
    assert table.partitions[6].name == "1337_jun_04"


def test_partitioning_time_monthly_auto_create_insert():
    """Tests whether automatically created monthly partitions line up
    perfectly."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    with freezegun.freeze_time("1337-01-01"):
        manager = PostgresPartitioningManager(
            [partition_by_time(model, months=1, count=2)]
        )
        manager.auto_create()

    model.objects.create(timestamp=datetime.date(1337, 1, 1))
    model.objects.create(timestamp=datetime.date(1337, 1, 31))
    model.objects.create(timestamp=datetime.date(1337, 2, 28))

    with transaction.atomic():
        with pytest.raises(IntegrityError):
            model.objects.create(timestamp=datetime.date(1337, 3, 1))
            model.objects.create(timestamp=datetime.date(1337, 3, 2))

    with freezegun.freeze_time("1337-01-01"):
        manager = PostgresPartitioningManager(
            [partition_by_time(model, months=1, count=3)]
        )
        manager.auto_create()

    model.objects.create(timestamp=datetime.date(1337, 3, 1))
    model.objects.create(timestamp=datetime.date(1337, 3, 2))


def test_partitioning_time_weekly_auto_create_insert():
    """Tests whether automatically created weekly partitions line up
    perfectly."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    # that's a monday
    with freezegun.freeze_time("1337-01-07"):
        manager = PostgresPartitioningManager(
            [partition_by_time(model, weeks=1, count=2)]
        )
        manager.auto_create()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 2

    model.objects.create(timestamp=datetime.date(1337, 1, 7))
    model.objects.create(timestamp=datetime.date(1337, 1, 14))
    model.objects.create(timestamp=datetime.date(1337, 1, 20))

    with transaction.atomic():
        with pytest.raises(IntegrityError):
            model.objects.create(timestamp=datetime.date(1337, 1, 21))
            model.objects.create(timestamp=datetime.date(1337, 1, 22))

    with freezegun.freeze_time("1337-01-07"):
        manager = PostgresPartitioningManager(
            [partition_by_time(model, weeks=1, count=3)]
        )
        manager.auto_create()

    model.objects.create(timestamp=datetime.date(1337, 1, 21))
    model.objects.create(timestamp=datetime.date(1337, 1, 22))


def test_partitioning_time_daily_auto_create_insert():
    """Tests whether automatically created daily partitions line up
    perfectly."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    # that's a monday
    with freezegun.freeze_time("1337-01-07"):
        manager = PostgresPartitioningManager(
            [partition_by_time(model, days=1, count=2)]
        )
        manager.auto_create()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 2

    model.objects.create(timestamp=datetime.date(1337, 1, 7))
    model.objects.create(timestamp=datetime.date(1337, 1, 8))

    with transaction.atomic():
        with pytest.raises(IntegrityError):
            model.objects.create(timestamp=datetime.date(1337, 1, 9))
            model.objects.create(timestamp=datetime.date(1337, 1, 10))

    with freezegun.freeze_time("1337-01-07"):
        manager = PostgresPartitioningManager(
            [partition_by_time(model, days=1, count=4)]
        )
        manager.auto_create()

    model.objects.create(timestamp=datetime.date(1337, 1, 9))
    model.objects.create(timestamp=datetime.date(1337, 1, 10))


def test_partitioning_time_switch_interval():
    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    # create partition for january
    with freezegun.freeze_time("1337-01-01"):
        manager = PostgresPartitioningManager(
            [partition_by_time(model, months=1, count=1)]
        )
        manager.auto_create()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 1

    # three weeks later, oh damn! many data, maybe weekly partitioning?
    # suprise! won't work... now we overlapping partitions
    with freezegun.freeze_time("1337-01-21"):
        with pytest.raises(ProgrammingError):
            with transaction.atomic():
                manager = PostgresPartitioningManager(
                    [partition_by_time(model, weeks=1, count=1)]
                )
                manager.auto_create()

        # try again, but specify a date to start from, end
        # of january... it'll skip creating any partitions
        # till that date..
        manager = PostgresPartitioningManager(
            [
                partition_by_time(
                    model,
                    weeks=1,
                    count=3,
                    start_from=datetime.date(1337, 1, 31),
                )
            ]
        )
        manager.auto_create()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 2
    assert table.partitions[0].name == "1337_jan"
    assert table.partitions[1].name == "1337_week_05"


@pytest.mark.parametrize(
    "kwargs,partition_names",
    [
        (dict(days=2), ["1337_jan_01", "1337_jan_03"]),
        (dict(weeks=2), ["1337_week_00", "1337_week_02"]),
        (dict(months=2), ["1337_jan", "1337_mar"]),
        (dict(years=2), ["1337", "1339"]),
    ],
)
def test_partitioning_time_multiple(kwargs, partition_names):
    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    with freezegun.freeze_time("1337-01-01"):
        manager = PostgresPartitioningManager(
            [partition_by_time(model, **kwargs, count=2)]
        )
        manager.auto_create()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 2
    assert partition_names == [par.name for par in table.partitions]


def test_partitioning_time_no_size():
    """Tests whether an error is raised when size for the partitions is
    specified."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    with pytest.raises(PostgresPartitioningError):
        partition_by_time(model, count=1)


def test_partitioning_time_multiple_sizes():
    """Tests whether an error is raised when multiple sizes for a partition are
    specified."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    with pytest.raises(PostgresPartitioningError):
        partition_by_time(model, weeks=1, months=2, count=1)
