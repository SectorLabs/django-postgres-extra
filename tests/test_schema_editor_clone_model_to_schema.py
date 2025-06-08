import os

from typing import Set, Tuple

import django
import pytest

from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.indexes import GinIndex
from django.db import connection, models, transaction
from django.db.models import Q

from psqlextra.backend.schema import PostgresSchemaEditor

from . import db_introspection
from .fake_model import delete_fake_model, get_fake_model

django_32_skip_reason = "Django < 3.2 can't support cloning models because it has hard coded references to the public schema"


def _create_schema() -> str:
    name = os.urandom(4).hex()

    with connection.cursor() as cursor:
        cursor.execute(
            "DROP SCHEMA IF EXISTS %s CASCADE"
            % connection.ops.quote_name(name),
            tuple(),
        )
        cursor.execute(
            "CREATE SCHEMA %s" % connection.ops.quote_name(name), tuple()
        )

    return name


@transaction.atomic
def _assert_cloned_table_is_same(
    source_table_fqn: Tuple[str, str],
    target_table_fqn: Tuple[str, str],
    excluding_constraints_and_indexes: bool = False,
):
    source_schema_name, source_table_name = source_table_fqn
    target_schema_name, target_table_name = target_table_fqn

    source_columns = db_introspection.get_columns(
        source_table_name, schema_name=source_schema_name
    )
    target_columns = db_introspection.get_columns(
        target_table_name, schema_name=target_schema_name
    )
    assert source_columns == target_columns

    source_relations = db_introspection.get_relations(
        source_table_name, schema_name=source_schema_name
    )
    target_relations = db_introspection.get_relations(
        target_table_name, schema_name=target_schema_name
    )
    if excluding_constraints_and_indexes:
        assert target_relations == {}
    else:
        assert source_relations == target_relations

    source_constraints = db_introspection.get_constraints(
        source_table_name, schema_name=source_schema_name
    )
    target_constraints = db_introspection.get_constraints(
        target_table_name, schema_name=target_schema_name
    )
    if excluding_constraints_and_indexes:
        assert target_constraints == {}
    else:
        assert source_constraints == target_constraints

    source_sequences = db_introspection.get_sequences(
        source_table_name, schema_name=source_schema_name
    )
    target_sequences = db_introspection.get_sequences(
        target_table_name, schema_name=target_schema_name
    )
    assert source_sequences == target_sequences

    source_storage_settings = db_introspection.get_storage_settings(
        source_table_name,
        schema_name=source_schema_name,
    )
    target_storage_settings = db_introspection.get_storage_settings(
        target_table_name, schema_name=target_schema_name
    )
    assert source_storage_settings == target_storage_settings


def _list_lock_modes_in_schema(schema_name: str) -> Set[str]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
              l.mode
            FROM pg_locks l
            INNER JOIN pg_class t ON t.oid = l.relation
            INNER JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE
                t.relnamespace >= 2200
                AND n.nspname = %s
            ORDER BY n.nspname, t.relname, l.mode
            """,
            (schema_name,),
        )

        return {lock_mode for lock_mode, in cursor.fetchall()}


def _clone_model_into_schema(model):
    schema_name = _create_schema()

    with PostgresSchemaEditor(connection) as schema_editor:
        schema_editor.clone_model_structure_to_schema(
            model, schema_name=schema_name
        )
        schema_editor.clone_model_constraints_and_indexes_to_schema(
            model, schema_name=schema_name
        )
        schema_editor.clone_model_foreign_keys_to_schema(
            model, schema_name=schema_name
        )

    return schema_name


@pytest.fixture
def fake_model_fk_target_1():
    model = get_fake_model(
        {
            "name": models.TextField(),
        },
    )

    yield model

    delete_fake_model(model)


@pytest.fixture
def fake_model_fk_target_2():
    model = get_fake_model(
        {
            "name": models.TextField(),
        },
    )

    yield model

    delete_fake_model(model)


@pytest.fixture
def fake_model(fake_model_fk_target_1, fake_model_fk_target_2):
    meta_options = {
        "indexes": [
            models.Index(fields=["age", "height"]),
            models.Index(fields=["age"], name="age_index"),
            GinIndex(fields=["nicknames"], name="nickname_index"),
        ],
        "constraints": [
            models.UniqueConstraint(
                fields=["first_name", "last_name"],
                name="first_last_name_uniq",
            ),
            models.CheckConstraint(
                check=Q(age__gt=0, height__gt=0), name="age_height_check"
            ),
        ],
        "unique_together": (
            "first_name",
            "nicknames",
        ),
    }

    if django.VERSION < (5, 1):
        meta_options["index_together"] = (
            "blob",
            "age",
        )

    model = get_fake_model(
        {
            "first_name": models.TextField(null=True),
            "last_name": models.TextField(),
            "age": models.PositiveIntegerField(),
            "height": models.FloatField(),
            "nicknames": ArrayField(base_field=models.TextField()),
            "blob": models.JSONField(),
            "family": models.ForeignKey(
                fake_model_fk_target_1, on_delete=models.CASCADE
            ),
            "alternative_family": models.ForeignKey(
                fake_model_fk_target_2, null=True, on_delete=models.SET_NULL
            ),
        },
        meta_options=meta_options,
    )

    yield model

    delete_fake_model(model)


@pytest.mark.skipif(
    django.VERSION < (3, 2),
    reason=django_32_skip_reason,
)
@pytest.mark.django_db(transaction=True)
def test_schema_editor_clone_model_to_schema(
    fake_model, fake_model_fk_target_1, fake_model_fk_target_2
):
    """Tests that cloning a model into a separate schema without obtaining
    AccessExclusiveLock on the source table works as expected."""

    schema_editor = PostgresSchemaEditor(connection)

    with schema_editor:
        schema_editor.alter_table_storage_setting(
            fake_model._meta.db_table, "autovacuum_enabled", "false"
        )

    table_name = fake_model._meta.db_table
    source_schema_name = "public"
    target_schema_name = _create_schema()

    with schema_editor:
        schema_editor.clone_model_structure_to_schema(
            fake_model, schema_name=target_schema_name
        )

        assert _list_lock_modes_in_schema(source_schema_name) == {
            "AccessShareLock"
        }

    _assert_cloned_table_is_same(
        (source_schema_name, table_name),
        (target_schema_name, table_name),
        excluding_constraints_and_indexes=True,
    )

    with schema_editor:
        schema_editor.clone_model_constraints_and_indexes_to_schema(
            fake_model, schema_name=target_schema_name
        )

        assert _list_lock_modes_in_schema(source_schema_name) == {
            "AccessShareLock",
            "ShareRowExclusiveLock",
        }

    _assert_cloned_table_is_same(
        (source_schema_name, table_name),
        (target_schema_name, table_name),
    )

    with schema_editor:
        schema_editor.clone_model_foreign_keys_to_schema(
            fake_model, schema_name=target_schema_name
        )

        assert _list_lock_modes_in_schema(source_schema_name) == {
            "AccessShareLock",
            "RowShareLock",
        }

    _assert_cloned_table_is_same(
        (source_schema_name, table_name),
        (target_schema_name, table_name),
    )


@pytest.mark.skipif(
    django.VERSION < (3, 2),
    reason=django_32_skip_reason,
)
def test_schema_editor_clone_model_to_schema_custom_constraint_names(
    fake_model, fake_model_fk_target_1
):
    """Tests that even if constraints were given custom names, the cloned table
    has those same custom names."""

    table_name = fake_model._meta.db_table
    source_schema_name = "public"

    constraints = db_introspection.get_constraints(table_name)

    primary_key_constraint = next(
        (
            name
            for name, constraint in constraints.items()
            if constraint["primary_key"]
        ),
        None,
    )
    foreign_key_constraint = next(
        (
            name
            for name, constraint in constraints.items()
            if constraint["foreign_key"]
            == (fake_model_fk_target_1._meta.db_table, "id")
        ),
        None,
    )
    check_constraint = next(
        (
            name
            for name, constraint in constraints.items()
            if constraint["check"] and constraint["columns"] == ["age"]
        ),
        None,
    )

    with connection.cursor() as cursor:
        cursor.execute(
            f"ALTER TABLE {table_name} RENAME CONSTRAINT {primary_key_constraint} TO custompkname"
        )
        cursor.execute(
            f"ALTER TABLE {table_name} RENAME CONSTRAINT {foreign_key_constraint} TO customfkname"
        )
        cursor.execute(
            f"ALTER TABLE {table_name} RENAME CONSTRAINT {check_constraint} TO customcheckname"
        )

    target_schema_name = _clone_model_into_schema(fake_model)

    _assert_cloned_table_is_same(
        (source_schema_name, table_name),
        (target_schema_name, table_name),
    )
