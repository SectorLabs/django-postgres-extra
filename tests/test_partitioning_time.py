import datetime

import freezegun
import pytest

from dateutil.relativedelta import relativedelta
from django.db import connection, models, transaction
from django.db.utils import IntegrityError

from psqlextra.partitioning import (
    PostgresPartitioningError,
    PostgresPartitioningManager,
    partition_by_current_time,
)

from . import db_introspection
from .fake_model import define_fake_partitioned_model


def _get_partitioned_table(model):
    return db_introspection.get_partitioned_table(model._meta.db_table)


@pytest.mark.postgres_version(lt=110000)
def test_partitioning_time_yearly_apply():
    """Tests whether automatically creating new partitions ahead yearly works
    as expected."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    with freezegun.freeze_time("2019-1-1"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, years=1, count=2)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 2
    assert table.partitions[0].name == "2019"
    assert table.partitions[1].name == "2020"

    with freezegun.freeze_time("2019-12-30"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, years=1, count=3)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 3
    assert table.partitions[0].name == "2019"
    assert table.partitions[1].name == "2020"
    assert table.partitions[2].name == "2021"


@pytest.mark.postgres_version(lt=110000)
def test_partitioning_time_monthly_apply():
    """Tests whether automatically creating new partitions ahead monthly works
    as expected."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    # create partitions for the next 12 months (including the current)
    with freezegun.freeze_time("2019-1-30"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, months=1, count=12)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 12
    assert table.partitions[0].name == "2019_jan"
    assert table.partitions[1].name == "2019_feb"
    assert table.partitions[2].name == "2019_mar"
    assert table.partitions[3].name == "2019_apr"
    assert table.partitions[4].name == "2019_may"
    assert table.partitions[5].name == "2019_jun"
    assert table.partitions[6].name == "2019_jul"
    assert table.partitions[7].name == "2019_aug"
    assert table.partitions[8].name == "2019_sep"
    assert table.partitions[9].name == "2019_oct"
    assert table.partitions[10].name == "2019_nov"
    assert table.partitions[11].name == "2019_dec"

    # re-running it with 13, should just create one additional partition
    with freezegun.freeze_time("2019-1-30"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, months=1, count=13)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 13
    assert table.partitions[12].name == "2020_jan"

    # it's november now, we only want to create 4 partitions ahead,
    # so only one new partition should be created for february 1338
    with freezegun.freeze_time("2019-11-1"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, months=1, count=4)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 14
    assert table.partitions[13].name == "2020_feb"


@pytest.mark.postgres_version(lt=110000)
def test_partitioning_time_monthly_with_custom_naming_apply():
    """Tests whether automatically created new partitions are named according
    to the specified name_format."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    # create partitions for the next 12 months (including the current)
    with freezegun.freeze_time("2019-1-30"):
        manager = PostgresPartitioningManager(
            [
                partition_by_current_time(
                    model, months=1, count=12, name_format="%Y_%m"
                )
            ]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 12
    assert table.partitions[0].name == "2019_01"
    assert table.partitions[1].name == "2019_02"
    assert table.partitions[2].name == "2019_03"
    assert table.partitions[3].name == "2019_04"
    assert table.partitions[4].name == "2019_05"
    assert table.partitions[5].name == "2019_06"
    assert table.partitions[6].name == "2019_07"
    assert table.partitions[7].name == "2019_08"
    assert table.partitions[8].name == "2019_09"
    assert table.partitions[9].name == "2019_10"
    assert table.partitions[10].name == "2019_11"
    assert table.partitions[11].name == "2019_12"


@pytest.mark.postgres_version(lt=110000)
def test_partitioning_time_weekly_apply():
    """Tests whether automatically creating new partitions ahead weekly works
    as expected."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    # create partitions for the next 4 weeks (including the current)
    with freezegun.freeze_time("2019-1-23"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, weeks=1, count=4)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 4
    assert table.partitions[0].name == "2019_week_03"
    assert table.partitions[1].name == "2019_week_04"
    assert table.partitions[2].name == "2019_week_05"
    assert table.partitions[3].name == "2019_week_06"

    # re-running it with 5, should just create one additional partition
    with freezegun.freeze_time("2019-1-23"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, weeks=1, count=5)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 5
    assert table.partitions[4].name == "2019_week_07"

    # it's june now, we want to partition two weeks ahead
    with freezegun.freeze_time("2019-06-03"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, weeks=1, count=2)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 7
    assert table.partitions[5].name == "2019_week_22"
    assert table.partitions[6].name == "2019_week_23"


@pytest.mark.postgres_version(lt=110000)
def test_partitioning_time_daily_apply():
    """Tests whether automatically creating new partitions ahead daily works as
    expected."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    # create partitions for the next 4 days (including the current)
    with freezegun.freeze_time("2019-1-23"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, days=1, count=4)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 4
    assert table.partitions[0].name == "2019_jan_23"
    assert table.partitions[1].name == "2019_jan_24"
    assert table.partitions[2].name == "2019_jan_25"
    assert table.partitions[3].name == "2019_jan_26"

    # re-running it with 5, should just create one additional partition
    with freezegun.freeze_time("2019-1-23"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, days=1, count=5)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 5
    assert table.partitions[4].name == "2019_jan_27"

    # it's june now, we want to partition two days ahead
    with freezegun.freeze_time("2019-06-03"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, days=1, count=2)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 7
    assert table.partitions[5].name == "2019_jun_03"
    assert table.partitions[6].name == "2019_jun_04"


@pytest.mark.postgres_version(lt=110000)
def test_partitioning_time_hourly_apply():
    """Tests whether automatically creating new partitions ahead hourly works
    as expected."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    # create partitions for the next 4 hours (including the current)
    with freezegun.freeze_time("2019-1-23"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, hours=1, count=4)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 4
    assert table.partitions[0].name == "2019_jan_23_00:00:00"
    assert table.partitions[1].name == "2019_jan_23_01:00:00"
    assert table.partitions[2].name == "2019_jan_23_02:00:00"
    assert table.partitions[3].name == "2019_jan_23_03:00:00"

    # re-running it with 5, should just create one additional partition
    with freezegun.freeze_time("2019-1-23"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, hours=1, count=5)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 5
    assert table.partitions[4].name == "2019_jan_23_04:00:00"

    # it's june now, we want to partition two hours ahead
    with freezegun.freeze_time("2019-06-03"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, hours=1, count=2)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 7
    assert table.partitions[5].name == "2019_jun_03_00:00:00"
    assert table.partitions[6].name == "2019_jun_03_01:00:00"


@pytest.mark.postgres_version(lt=110000)
def test_partitioning_time_consistent_daily_apply():
    """Ensures that automatic daily partition creation is consistent and
    aligned when the partition size spans multiple days (e.g., days > 1)"""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    with freezegun.freeze_time("2025-06-20"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, days=5, count=3)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 3

    # Partitions are aligned based on the fixed anchor (Unix Epoch by default).
    # 2025-06-20 falls within the partition starting at 2025-06-16,
    # since it's the most recent multiple of 5 days since 1970-01-01.
    assert table.partitions[0].name == "2025_jun_16"
    assert table.partitions[1].name == "2025_jun_21"
    assert table.partitions[2].name == "2025_jun_26"

    # re-running it another day only creates the next one needed.
    with freezegun.freeze_time("2025-06-22"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, days=5, count=3)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 4
    assert table.partitions[0].name == "2025_jun_16"
    assert table.partitions[1].name == "2025_jun_21"
    assert table.partitions[2].name == "2025_jun_26"
    assert table.partitions[3].name == "2025_jul_01"


@pytest.mark.postgres_version(lt=110000)
def test_partitioning_time_monthly_apply_insert():
    """Tests whether automatically created monthly partitions line up
    perfectly."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    with freezegun.freeze_time("2019-1-1"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, months=1, count=2)]
        )
        manager.plan().apply()

    model.objects.create(timestamp=datetime.date(2019, 1, 1))
    model.objects.create(timestamp=datetime.date(2019, 1, 31))
    model.objects.create(timestamp=datetime.date(2019, 2, 28))

    with transaction.atomic():
        with pytest.raises(IntegrityError):
            model.objects.create(timestamp=datetime.date(2019, 3, 1))
            model.objects.create(timestamp=datetime.date(2019, 3, 2))

    with freezegun.freeze_time("2019-1-1"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, months=1, count=3)]
        )
        manager.plan().apply()

    model.objects.create(timestamp=datetime.date(2019, 3, 1))
    model.objects.create(timestamp=datetime.date(2019, 3, 2))


@pytest.mark.postgres_version(lt=110000)
def test_partitioning_time_weekly_apply_insert():
    """Tests whether automatically created weekly partitions line up
    perfectly."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    # that's a monday
    with freezegun.freeze_time("2019-1-08"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, weeks=1, count=2)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 2

    model.objects.create(timestamp=datetime.date(2019, 1, 7))
    model.objects.create(timestamp=datetime.date(2019, 1, 14))
    model.objects.create(timestamp=datetime.date(2019, 1, 20))

    with transaction.atomic():
        with pytest.raises(IntegrityError):
            model.objects.create(timestamp=datetime.date(2019, 1, 21))
            model.objects.create(timestamp=datetime.date(2019, 1, 22))

    with freezegun.freeze_time("2019-1-07"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, weeks=1, count=3)]
        )
        manager.plan().apply()

    model.objects.create(timestamp=datetime.date(2019, 1, 21))
    model.objects.create(timestamp=datetime.date(2019, 1, 22))


@pytest.mark.postgres_version(lt=110000)
def test_partitioning_time_daily_apply_insert():
    """Tests whether automatically created daily partitions line up
    perfectly."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    # that's a monday
    with freezegun.freeze_time("2019-1-07"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, days=1, count=2)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 2

    model.objects.create(timestamp=datetime.date(2019, 1, 7))
    model.objects.create(timestamp=datetime.date(2019, 1, 8))

    with transaction.atomic():
        with pytest.raises(IntegrityError):
            model.objects.create(timestamp=datetime.date(2019, 1, 9))
            model.objects.create(timestamp=datetime.date(2019, 1, 10))

    with freezegun.freeze_time("2019-1-07"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, days=1, count=4)]
        )
        manager.plan().apply()

    model.objects.create(timestamp=datetime.date(2019, 1, 9))
    model.objects.create(timestamp=datetime.date(2019, 1, 10))


@pytest.mark.postgres_version(lt=110000)
def test_partitioning_time_hourly_apply_insert():
    """Tests whether automatically created hourly partitions line up
    perfectly."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    # that's a monday
    with freezegun.freeze_time("2019-1-07"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, hours=1, count=2)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 2

    model.objects.create(timestamp=datetime.datetime(2019, 1, 7, 0))
    model.objects.create(timestamp=datetime.datetime(2019, 1, 7, 1))

    with transaction.atomic():
        with pytest.raises(IntegrityError):
            model.objects.create(timestamp=datetime.datetime(2019, 1, 7, 2))
            model.objects.create(timestamp=datetime.datetime(2019, 1, 7, 3))

    with freezegun.freeze_time("2019-1-07"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, hours=1, count=4)]
        )
        manager.plan().apply()

    model.objects.create(timestamp=datetime.datetime(2019, 1, 7, 2))
    model.objects.create(timestamp=datetime.datetime(2019, 1, 7, 3))


@pytest.mark.postgres_version(lt=110000)
@pytest.mark.parametrize(
    "kwargs,partition_names",
    [
        (dict(days=2), ["2018_dec_31", "2019_jan_02"]),
        (dict(hours=2), ["2019_jan_01_00:00:00", "2019_jan_01_02:00:00"]),
        (dict(weeks=2), ["2018_week_53", "2019_week_02"]),
        (dict(months=2), ["2019_jan", "2019_mar"]),
        (dict(years=2), ["2019", "2021"]),
    ],
)
def test_partitioning_time_multiple(kwargs, partition_names):
    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    with freezegun.freeze_time("2019-1-1"):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, **kwargs, count=2)]
        )
        manager.plan().apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 2
    assert partition_names == [par.name for par in table.partitions]


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
def test_partitioning_time_delete(kwargs, timepoints):
    """Tests whether partitions older than the specified max_age are
    automatically deleted."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    partition_kwargs = {"model": model, "count": 6, **kwargs}

    manager = PostgresPartitioningManager(
        [partition_by_current_time(**partition_kwargs)]
    )

    with freezegun.freeze_time(timepoints[0][0]):
        manager.plan().apply()

    for (dt, partition_count) in timepoints:
        with freezegun.freeze_time(dt):
            manager.plan(skip_create=True).apply()

            table = _get_partitioned_table(model)
            assert len(table.partitions) == partition_count


@pytest.mark.postgres_version(lt=110000)
def test_partitioning_time_delete_ignore_manual():
    """Tests whether partitions that were created manually are ignored.

    Partitions created automatically have a special comment attached to
    them. Only partitions with this special comments would be deleted.
    """

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    manager = PostgresPartitioningManager(
        [partition_by_current_time(model, count=2, months=1)]
    )

    schema_editor.add_range_partition(
        model, "2019_jan", from_values="2019-1-1", to_values="2019-2-1"
    )

    with freezegun.freeze_time("2020-1-1"):
        manager.plan(skip_create=True).apply()

    table = _get_partitioned_table(model)
    assert len(table.partitions) == 1


def test_partitioning_time_no_size():
    """Tests whether an error is raised when size for the partitions is
    specified."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    with pytest.raises(PostgresPartitioningError):
        partition_by_current_time(model, count=1)


def test_partitioning_time_multiple_sizes():
    """Tests whether an error is raised when multiple sizes for a partition are
    specified."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    with pytest.raises(PostgresPartitioningError):
        partition_by_current_time(model, weeks=1, months=2, count=1)


def _count_rows(table_name):
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT COUNT(*) FROM {connection.ops.quote_name(table_name)}"
        )
        return cursor.fetchone()[0]


def _create_and_fill_monthly(model):
    schema_editor = connection.schema_editor()
    schema_editor.create_partitioned_model(model)

    manager = PostgresPartitioningManager(
        [
            partition_by_current_time(
                model, months=1, count=3, max_age=relativedelta(months=2)
            )
        ]
    )

    with freezegun.freeze_time("2019-1-1"):
        manager.plan().apply()

    model.objects.create(timestamp=datetime.datetime(2019, 1, 10))
    model.objects.create(timestamp=datetime.datetime(2019, 2, 10))
    model.objects.create(timestamp=datetime.datetime(2019, 3, 10))

    return manager


@pytest.mark.postgres_version(lt=110000)
def test_partitioning_time_detach():
    """Tests whether partitions older than max_age are detached instead of
    dropped, keeping their tables and rows."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )
    manager = _create_and_fill_monthly(model)

    with freezegun.freeze_time("2019-3-1"):
        plan = manager.plan(skip_create=True)
        assert [p.name() for p in plan.deletions] == ["2019_jan"]
        plan.apply(detach=True)

    table = _get_partitioned_table(model)
    assert [p.name for p in table.partitions] == ["2019_feb", "2019_mar"]

    detached_table_name = f"{model._meta.db_table}_2019_jan"
    assert detached_table_name in db_introspection.table_names()
    assert _count_rows(detached_table_name) == 1
    assert model.objects.count() == 2

    # detached partitions are not partitions anymore, so running the plan
    # again does not touch them
    with freezegun.freeze_time("2019-3-1"):
        assert manager.plan(skip_create=True).deletions == []


@pytest.mark.postgres_version(lt=140000)
@pytest.mark.django_db(transaction=True)
def test_partitioning_time_detach_concurrently():
    """Tests whether partitions can be detached concurrently while new
    partitions are created in the same plan."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )
    manager = _create_and_fill_monthly(model)

    with freezegun.freeze_time("2019-3-1"):
        plan = manager.plan()
        assert [p.name() for p in plan.creations] == ["2019_apr", "2019_may"]
        assert [p.name() for p in plan.deletions] == ["2019_jan"]
        plan.apply(concurrently=True)

    with transaction.atomic():
        table = _get_partitioned_table(model)
    assert sorted(p.name for p in table.partitions) == [
        "2019_apr",
        "2019_feb",
        "2019_mar",
        "2019_may",
    ]

    detached_table_name = f"{model._meta.db_table}_2019_jan"
    assert _count_rows(detached_table_name) == 1
    assert model.objects.count() == 2

    with connection.schema_editor(atomic=False) as schema_editor:
        schema_editor.delete_partitioned_model(model)
        schema_editor.execute(
            f"DROP TABLE {connection.ops.quote_name(detached_table_name)}"
        )
