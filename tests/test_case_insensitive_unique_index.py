import pytest

from django.db import IntegrityError, connection, models
from django.db.migrations import AddIndex, CreateModel

from psqlextra.indexes import CaseInsensitiveUniqueIndex
from psqlextra.models import PostgresModel

from .fake_model import get_fake_model
from .migrations import apply_migration, filtered_schema_editor


def test_ciui_migrations():
    """Tests whether migrations for case sensitive indexes are being created as
    expected."""

    index_1 = CaseInsensitiveUniqueIndex(
        fields=["name", "other_name"], name="index1"
    )

    ops = [
        CreateModel(
            name="mymodel",
            fields=[
                ("name", models.CharField(max_length=255)),
                ("other_name", models.CharField(max_length=255)),
            ],
        ),
        AddIndex(model_name="mymodel", index=index_1),
    ]

    with filtered_schema_editor("CREATE UNIQUE INDEX") as calls:
        apply_migration(ops)

    sql = str([call[0] for _, call, _ in calls["CREATE UNIQUE INDEX"]][0])
    expected_sql = 'CREATE UNIQUE INDEX "index1" ON "tests_mymodel" (LOWER("name"), LOWER("other_name"))'
    assert sql == expected_sql


def test_ciui():
    """Tests whether the case insensitive unique index works as expected."""

    index_1 = CaseInsensitiveUniqueIndex(fields=["name"], name="index1")

    model = get_fake_model(
        {"name": models.CharField(max_length=255)}, PostgresModel
    )

    with connection.schema_editor() as schema_editor:
        schema_editor.add_index(model, index_1)

    model.objects.create(name="henk")

    with pytest.raises(IntegrityError):
        model.objects.create(name="Henk")


def test_ciui_on_conflict():
    """Tests wether fields with a :see:CaseInsensitiveUniqueIndex can be used
    as a conflict target."""

    index_1 = CaseInsensitiveUniqueIndex(fields=["name"], name="index1")

    model = get_fake_model(
        {"name": models.CharField(max_length=255)},
        PostgresModel,
        {"indexes": [index_1]},
    )

    model.objects.upsert(conflict_target=["name"], fields=dict(name="henk"))
