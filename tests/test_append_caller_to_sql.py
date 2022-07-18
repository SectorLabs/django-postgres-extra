import pytest

from django.db import connection, models
from django.test.utils import CaptureQueriesContext, override_settings

from psqlextra.compiler import append_caller_to_sql

from .fake_model import get_fake_model


class psqlextraSimulated:
    def callMockedClass(self):
        return MockedClass().mockedMethod()


class MockedClass:
    def mockedMethod(self):
        return append_caller_to_sql("sql")


def mockedFunction():
    return append_caller_to_sql("sql")


@override_settings(POSTGRES_EXTRA_ANNOTATE_SQL=False)
def test_disable_append_caller_to_sql():
    commented_sql = mockedFunction()
    assert commented_sql == "sql"


@pytest.mark.parametrize(
    "entry_point",
    [
        MockedClass().mockedMethod,
        psqlextraSimulated().callMockedClass,
    ],
)
@override_settings(POSTGRES_EXTRA_ANNOTATE_SQL=True)
def test_append_caller_to_sql_class(entry_point):
    commented_sql = entry_point()
    assert commented_sql.startswith("sql /* ")
    assert "mockedMethod" in commented_sql
    assert __file__ in commented_sql


@override_settings(POSTGRES_EXTRA_ANNOTATE_SQL=True)
def test_append_caller_to_sql_function():
    commented_sql = mockedFunction()
    assert commented_sql.startswith("sql /* ")
    assert "mockedFunction" in commented_sql
    assert __file__ in commented_sql


@override_settings(POSTGRES_EXTRA_ANNOTATE_SQL=True)
def test_append_caller_to_sql_crud():
    model = get_fake_model(
        {
            "title": models.CharField(max_length=255, null=True),
        }
    )

    obj = None
    with CaptureQueriesContext(connection) as queries:
        obj = model.objects.create(
            id=1,
            title="Test",
        )
        assert "test_append_caller_to_sql_crud " in queries[0]["sql"]

    obj.title = "success"
    with CaptureQueriesContext(connection) as queries:
        obj.save()
        assert "test_append_caller_to_sql_crud " in queries[0]["sql"]

    with CaptureQueriesContext(connection) as queries:
        assert model.objects.filter(id=obj.id)[0].id == obj.id
        assert "test_append_caller_to_sql_crud " in queries[0]["sql"]

    with CaptureQueriesContext(connection) as queries:
        obj.delete()
        assert "test_append_caller_to_sql_crud " in queries[0]["sql"]
