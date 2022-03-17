import pytest

from django.db import connection, models
from django.db.models import CheckConstraint, Q, UniqueConstraint

from psqlextra.backend.schema import PostgresSchemaEditor

from .fake_model import get_fake_model


def test_schema_editor_add_constraint_not_valid():
    model = get_fake_model({"name": models.TextField()})
    constraint = CheckConstraint(name="mycheck", check=Q(name__gt=2))

    schema_editor = PostgresSchemaEditor(connection, collect_sql=True)
    schema_editor.add_constraint_not_valid(model, constraint)

    assert schema_editor.collected_sql == [
        'ALTER TABLE "%s" ADD CONSTRAINT "mycheck" CHECK ("name" > \'2\') NOT VALID;'
        % model._meta.db_table
    ]


def test_schema_editor_add_constraint_not_valid_non_check_constraint():
    model = get_fake_model({"name": models.TextField()})
    constraint = UniqueConstraint(name="myunique", fields=["a"])

    schema_editor = PostgresSchemaEditor(connection, collect_sql=True)

    with pytest.raises(TypeError):
        schema_editor.add_constraint_not_valid(model, constraint)

    assert schema_editor.collected_sql == []


def test_schema_editor_validate_constraint():
    model = get_fake_model({"name": models.TextField()})
    constraint = CheckConstraint(name="mycheck", check=Q(name__gt=2))

    schema_editor = PostgresSchemaEditor(connection, collect_sql=True)
    schema_editor.validate_constraint(model, constraint)

    assert schema_editor.collected_sql == [
        'ALTER TABLE "%s" VALIDATE CONSTRAINT "mycheck";' % model._meta.db_table
    ]
