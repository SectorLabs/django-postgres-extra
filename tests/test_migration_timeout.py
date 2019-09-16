import time
import typing

import pytest

from django.apps import apps
from django.db import (
    IntegrityError,
    connection,
    migrations,
    models,
    transaction,
)
from django.db.migrations import CreateModel
from django.db.migrations.executor import MigrationExecutor
from django.db.utils import InterfaceError as DjangoInterfaceError
from django.db.utils import OperationalError
from psycopg2 import InterfaceError as Psycopg2InterfaceError

import psqlextra.indexes.conditional_unique_index

from psqlextra.backend.migrations.patched_migrations import MigrationWithTimeout
from tests.fake_model import define_fake_model
from tests.migrations import apply_migration


@pytest.fixture(autouse=True)
def establish_connection():
    """Fixture used for tests which mess up the connection to the database."""
    yield
    connection.ensure_connection()


def apply_patched_migration_with_timeout(
    operations,
    state=None,
    backwards: bool = False,
    safe_interrupt: bool = True,
    timeout: float = None,
    cancel_method: MigrationWithTimeout.CancellationMethod = MigrationWithTimeout.CancellationMethod.SQL,
):
    """Executes the specified migration operations using the specified schema
    editor.

    Arguments:
        operations:
            The migration operations to execute.

        state:
            The state state to use during the
            migrations.

        backwards:
            Whether to apply the operations
            in reverse (backwards).

        timeout:
            Cancel the operation if it takes
            more than x seconds

        safe_interrupt:
            Cancel safely or kill the connection
            entirely

        cancel_method:
            Tells the migration class how to
            abort the currently running operation
    """

    state = state or migrations.state.ProjectState.from_apps(apps)

    MigrationWithTimeout.operations = operations

    migration = MigrationWithTimeout(
        "migration", "tests", safe_interrupt=safe_interrupt
    )
    migration.timeout = timeout
    migration.cancellation_method = cancel_method

    executor = MigrationExecutor(connection)

    if not backwards:
        executor.apply_migration(state, migration)
    else:
        executor.unapply_migration(state, migration)

    return migration


def expectation_judge(
    expect_exception: bool,
    exception_expected: typing.Union[
        typing.Type[Exception],
        typing.Tuple[
            typing.Type[Exception],
            typing.Type[Exception],
            typing.Type[Exception],
        ],
    ],
    func: callable,
    *args,
    with_transaction_wrapper=False,
    **kwargs,
):
    try:
        if expect_exception:
            with pytest.raises(exception_expected):
                if with_transaction_wrapper:
                    with transaction.atomic():
                        func(*args, **kwargs)
                else:
                    func(*args, **kwargs)
        else:
            if with_transaction_wrapper:
                with transaction.atomic():
                    func(*args, **kwargs)
            else:
                func(*args, **kwargs)
    except KeyboardInterrupt:
        assert False


@pytest.mark.parametrize(
    "timeout, stalling_time, expect_interruption",
    [(0.25, 0.5, True), (None, 0.2, False), (0.5, 0.25, False)],
)
def test_migration_timeout_python_code(
    timeout, stalling_time, expect_interruption
):
    """Test migration timeout if running python code."""

    def stall(app_name, schema_editor):
        time.sleep(stalling_time)

    expectation_judge(
        expect_interruption,
        KeyboardInterrupt,
        apply_patched_migration_with_timeout,
        [migrations.RunPython(stall)],
        timeout=timeout,
        cancel_method=MigrationWithTimeout.CancellationMethod.PYTHON,
    )


@pytest.mark.parametrize(
    "timeout, stalling_time, expect_interruption",
    [(0.25, 0.5, True), (None, 0.2, False), (0.5, 0.25, False)],
)
def test_migration_timeout_add_index(
    timeout, stalling_time, expect_interruption
):
    """Test add index."""
    field_name = "model_id"
    objects_added = 100
    model_fields = {field_name: models.IntegerField}

    def prepare_fields():
        return {name: field() for name, field in model_fields.items()}

    model = define_fake_model(fields=prepare_fields())
    apply_migration([CreateModel(model.__name__, prepare_fields().items())])

    model.objects.bulk_create(
        [model(**{field_name: x}) for x in range(objects_added)]
    )

    assert model.objects.all().count() == objects_added

    operations = [
        migrations.RunSQL(f"select pg_sleep({stalling_time});"),
        migrations.AddIndex(
            model_name=model.__name__,
            index=psqlextra.indexes.conditional_unique_index.ConditionalUniqueIndex(
                condition=f'"{field_name}" IS NOT NULL',
                fields=[field_name],
                name=f"{field_name}_idx",
            ),
        ),
    ]

    expectation_judge(
        expect_interruption,
        OperationalError,
        apply_patched_migration_with_timeout,
        operations,
        timeout=timeout,
    )
    expectation_judge(
        not expect_interruption,
        IntegrityError,
        model.objects.bulk_create,
        [model(**{field_name: 0})],
        with_transaction_wrapper=True,
    )

    assert model.objects.all().count() == objects_added + expect_interruption


@pytest.mark.parametrize(
    "timeout, stalling_time, expect_interruption",
    [(0.25, 0.5, True), (0.2, 0.1, False)],
)
def test_migration_timeout_force_close_sql_connection(
    timeout, stalling_time, expect_interruption, establish_connection
):
    assert connection.is_usable()

    expectation_judge(
        expect_interruption,
        (OperationalError, Psycopg2InterfaceError, DjangoInterfaceError),
        apply_patched_migration_with_timeout,
        [migrations.RunSQL(f"select pg_sleep({stalling_time});")],
        timeout=timeout,
        cancel_method=MigrationWithTimeout.CancellationMethod.SQL,
        safe_interrupt=False,
    )

    assert connection.is_usable() != expect_interruption

    expectation_judge(
        expect_interruption,
        DjangoInterfaceError,
        apply_patched_migration_with_timeout,
        [migrations.RunSQL(f"select pg_sleep({stalling_time});")],
        timeout=timeout,
        cancel_method=MigrationWithTimeout.CancellationMethod.SQL,
    )


@pytest.mark.parametrize(
    "timeout, stalling_time, expect_interruption",
    [(0.25, 0.5, True), (None, 0.2, False), (0.5, 0.1, False)],
)
def test_migration_timeout_both_cancelling_methods_active(
    timeout, stalling_time, expect_interruption
):
    """Test migration timeout if running python code."""

    def stall(app_name, schema_editor):
        time.sleep(stalling_time)

    expectation_judge(
        expect_interruption,
        KeyboardInterrupt,
        apply_patched_migration_with_timeout,
        [
            migrations.RunSQL(f"select pg_sleep({stalling_time});"),
            migrations.RunPython(stall),
        ],
        timeout=timeout,
        cancel_method=MigrationWithTimeout.CancellationMethod.BOTH,
    )
