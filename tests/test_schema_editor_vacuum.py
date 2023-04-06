import pytest

from django.core.exceptions import SuspiciousOperation
from django.db import connection, models
from django.test.utils import CaptureQueriesContext

from psqlextra.backend.schema import PostgresSchemaEditor

from .fake_model import delete_fake_model, get_fake_model


@pytest.fixture
def fake_model():
    model = get_fake_model(
        {
            "name": models.TextField(),
        }
    )

    yield model

    delete_fake_model(model)


@pytest.fixture
def fake_model_non_concrete_field(fake_model):
    model = get_fake_model(
        {
            "fk": models.ForeignKey(
                fake_model, on_delete=models.CASCADE, related_name="fakes"
            ),
        }
    )

    yield model

    delete_fake_model(model)


def test_schema_editor_vacuum_not_in_transaction(fake_model):
    schema_editor = PostgresSchemaEditor(connection)

    with pytest.raises(SuspiciousOperation):
        schema_editor.vacuum_table(fake_model._meta.db_table)


@pytest.mark.parametrize(
    "kwargs,query",
    [
        (dict(), "VACUUM %s"),
        (dict(full=True), "VACUUM (FULL) %s"),
        (dict(analyze=True), "VACUUM (ANALYZE) %s"),
        (dict(parallel=8), "VACUUM (PARALLEL 8) %s"),
        (dict(analyze=True, verbose=True), "VACUUM (VERBOSE, ANALYZE) %s"),
        (
            dict(analyze=True, parallel=8, verbose=True),
            "VACUUM (VERBOSE, ANALYZE, PARALLEL 8) %s",
        ),
        (dict(freeze=True), "VACUUM (FREEZE) %s"),
        (dict(verbose=True), "VACUUM (VERBOSE) %s"),
        (dict(disable_page_skipping=True), "VACUUM (DISABLE_PAGE_SKIPPING) %s"),
        (dict(skip_locked=True), "VACUUM (SKIP_LOCKED) %s"),
        (dict(index_cleanup=True), "VACUUM (INDEX_CLEANUP) %s"),
        (dict(truncate=True), "VACUUM (TRUNCATE) %s"),
    ],
)
@pytest.mark.django_db(transaction=True)
def test_schema_editor_vacuum_table(fake_model, kwargs, query):
    schema_editor = PostgresSchemaEditor(connection)

    with CaptureQueriesContext(connection) as ctx:
        schema_editor.vacuum_table(fake_model._meta.db_table, **kwargs)

    queries = [query["sql"] for query in ctx.captured_queries]
    assert queries == [
        query % connection.ops.quote_name(fake_model._meta.db_table)
    ]


@pytest.mark.django_db(transaction=True)
def test_schema_editor_vacuum_table_columns(fake_model):
    schema_editor = PostgresSchemaEditor(connection)

    with CaptureQueriesContext(connection) as ctx:
        schema_editor.vacuum_table(
            fake_model._meta.db_table, ["id", "name"], analyze=True
        )

    queries = [query["sql"] for query in ctx.captured_queries]
    assert queries == [
        'VACUUM (ANALYZE) %s ("id", "name")'
        % connection.ops.quote_name(fake_model._meta.db_table)
    ]


@pytest.mark.django_db(transaction=True)
def test_schema_editor_vacuum_model(fake_model):
    schema_editor = PostgresSchemaEditor(connection)

    with CaptureQueriesContext(connection) as ctx:
        schema_editor.vacuum_model(fake_model, analyze=True, parallel=8)

    queries = [query["sql"] for query in ctx.captured_queries]
    assert queries == [
        "VACUUM (ANALYZE, PARALLEL 8) %s"
        % connection.ops.quote_name(fake_model._meta.db_table)
    ]


@pytest.mark.django_db(transaction=True)
def test_schema_editor_vacuum_model_fields(fake_model):
    schema_editor = PostgresSchemaEditor(connection)

    with CaptureQueriesContext(connection) as ctx:
        schema_editor.vacuum_model(
            fake_model,
            [fake_model._meta.get_field("name")],
            analyze=True,
            parallel=8,
        )

    queries = [query["sql"] for query in ctx.captured_queries]
    assert queries == [
        'VACUUM (ANALYZE, PARALLEL 8) %s ("name")'
        % connection.ops.quote_name(fake_model._meta.db_table)
    ]


@pytest.mark.django_db(transaction=True)
def test_schema_editor_vacuum_model_non_concrete_fields(
    fake_model, fake_model_non_concrete_field
):
    schema_editor = PostgresSchemaEditor(connection)

    with CaptureQueriesContext(connection) as ctx:
        schema_editor.vacuum_model(
            fake_model,
            [fake_model._meta.get_field("fakes")],
            analyze=True,
            parallel=8,
        )

    queries = [query["sql"] for query in ctx.captured_queries]
    assert queries == [
        "VACUUM (ANALYZE, PARALLEL 8) %s"
        % connection.ops.quote_name(fake_model._meta.db_table)
    ]
