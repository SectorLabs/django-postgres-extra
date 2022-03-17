from unittest.mock import call

import pytest

from django.db import models
from django.db.models import CheckConstraint, Q, UniqueConstraint

from psqlextra.backend.migrations.operations import (
    PostgresAddCheckConstraintConcurrently,
)

from .fake_model import get_fake_model
from .migrations import apply_migration, filtered_schema_editor


def test_add_check_constraint_concurrently():
    model = get_fake_model({"name": models.TextField()})
    constraint = CheckConstraint(name="mycheck", check=Q(name__gt=2))

    operation = PostgresAddCheckConstraintConcurrently(
        model_name=model.__name__,
        constraint=constraint,
    )

    assert not operation.atomic

    with filtered_schema_editor("NOT VALID", "VALIDATE") as calls:
        apply_migration([operation])

    assert calls["NOT VALID"] == [
        call(
            'ALTER TABLE "%s" ADD CONSTRAINT "mycheck" CHECK ("name" > \'2\') NOT VALID;'
            % model._meta.db_table,
            params=None,
        )
    ]
    assert calls["VALIDATE"] == [
        call(
            'ALTER TABLE "%s" VALIDATE CONSTRAINT "mycheck"'
            % model._meta.db_table,
            params=None,
        )
    ]


def test_add_check_constraint_concurrently_not_check():
    with pytest.raises(TypeError):
        PostgresAddCheckConstraintConcurrently(
            model_name="mymodel",
            constraint=UniqueConstraint(name="myunique", fields=["a"]),
        )
