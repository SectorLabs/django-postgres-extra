import pytest

from django.core.exceptions import SuspiciousOperation
from django.db import connection

from psqlextra.settings import (
    postgres_prepend_local_search_path,
    postgres_reset_local_search_path,
    postgres_set_local,
    postgres_set_local_search_path,
)


def _get_current_setting(name: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute(f"SHOW {name}")
        return cursor.fetchone()[0]


@postgres_set_local(statement_timeout="2s", lock_timeout="3s")
def test_postgres_set_local_function_decorator():
    assert _get_current_setting("statement_timeout") == "2s"
    assert _get_current_setting("lock_timeout") == "3s"


def test_postgres_set_local_context_manager():
    with postgres_set_local(statement_timeout="2s"):
        assert _get_current_setting("statement_timeout") == "2s"

    assert _get_current_setting("statement_timeout") == "0"


def test_postgres_set_local_iterable():
    with postgres_set_local(search_path=["a", "public"]):
        assert _get_current_setting("search_path") == "a, public"

    assert _get_current_setting("search_path") == '"$user", public'


def test_postgres_set_local_nested():
    with postgres_set_local(statement_timeout="2s"):
        assert _get_current_setting("statement_timeout") == "2s"

        with postgres_set_local(statement_timeout="3s"):
            assert _get_current_setting("statement_timeout") == "3s"

        assert _get_current_setting("statement_timeout") == "2s"

    assert _get_current_setting("statement_timeout") == "0"


@pytest.mark.django_db(transaction=True)
def test_postgres_set_local_no_transaction():
    with pytest.raises(SuspiciousOperation):
        with postgres_set_local(statement_timeout="2s"):
            pass


def test_postgres_set_local_search_path():
    with postgres_set_local_search_path(["a", "public"]):
        assert _get_current_setting("search_path") == "a, public"

    assert _get_current_setting("search_path") == '"$user", public'


def test_postgres_reset_local_search_path():
    with postgres_set_local_search_path(["a", "public"]):
        with postgres_reset_local_search_path():
            assert _get_current_setting("search_path") == '"$user", public'

        assert _get_current_setting("search_path") == "a, public"

    assert _get_current_setting("search_path") == '"$user", public'


def test_postgres_prepend_local_search_path():
    with postgres_prepend_local_search_path(["a", "b"]):
        assert _get_current_setting("search_path") == 'a, b, "$user", public'

    assert _get_current_setting("search_path") == '"$user", public'


def test_postgres_prepend_local_search_path_nested():
    with postgres_prepend_local_search_path(["a", "b"]):
        with postgres_prepend_local_search_path(["c"]):
            assert (
                _get_current_setting("search_path")
                == 'c, a, b, "$user", public'
            )

        assert _get_current_setting("search_path") == 'a, b, "$user", public'

    assert _get_current_setting("search_path") == '"$user", public'
