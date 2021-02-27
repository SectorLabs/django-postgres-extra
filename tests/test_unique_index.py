from django.db import models
from django.db.migrations import AddIndex, CreateModel

from psqlextra.indexes import UniqueIndex

from .migrations import apply_migration, filtered_schema_editor


def test_unique_index_migrations():
    index = UniqueIndex(fields=["name", "other_name"], name="index1")

    ops = [
        CreateModel(
            name="mymodel",
            fields=[
                ("name", models.TextField()),
                ("other_name", models.TextField()),
            ],
            options={
                # "indexes": [index],
            },
        ),
        AddIndex(model_name="mymodel", index=index),
    ]

    with filtered_schema_editor("CREATE UNIQUE INDEX") as calls:
        apply_migration(ops)

    calls = [call[0] for _, call, _ in calls["CREATE UNIQUE INDEX"]]

    db_table = "tests_mymodel"
    query = 'CREATE UNIQUE INDEX "index1" ON "{0}" ("name", "other_name")'
    assert str(calls[0]) == query.format(db_table)
