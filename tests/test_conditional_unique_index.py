import pytest

from django.db import IntegrityError, models, transaction
from django.db.migrations import AddIndex, CreateModel

from psqlextra.indexes import ConditionalUniqueIndex

from .fake_model import get_fake_model
from .migrations import apply_migration, filtered_schema_editor


def test_cui_deconstruct():
    """Tests whether the :see:ConditionalUniqueIndex's deconstruct() method
    works properly."""

    original_kwargs = dict(
        condition="field IS NULL", name="great_index", fields=["field", "build"]
    )
    _, _, new_kwargs = ConditionalUniqueIndex(**original_kwargs).deconstruct()

    for key, value in original_kwargs.items():
        assert new_kwargs[key] == value


def test_cui_migrations():
    """Tests whether the migrations are properly generated and executed."""

    index_1 = ConditionalUniqueIndex(
        fields=["name", "other_name"],
        condition='"name" IS NOT NULL',
        name="index1",
    )

    index_2 = ConditionalUniqueIndex(
        fields=["other_name"], condition='"name" IS NULL', name="index2"
    )

    ops = [
        CreateModel(
            name="mymodel",
            fields=[
                ("id", models.IntegerField(primary_key=True)),
                ("name", models.CharField(max_length=255, null=True)),
                ("other_name", models.CharField(max_length=255)),
            ],
            options={
                # "indexes": [index_1, index_2],
            },
        ),
        AddIndex(model_name="mymodel", index=index_1),
        AddIndex(model_name="mymodel", index=index_2),
    ]

    with filtered_schema_editor("CREATE UNIQUE INDEX") as calls:
        apply_migration(ops)

    calls = [call[0] for _, call, _ in calls["CREATE UNIQUE INDEX"]]

    db_table = "tests_mymodel"
    query = 'CREATE UNIQUE INDEX "index1" ON "{0}" ("name", "other_name") WHERE "name" IS NOT NULL'
    assert str(calls[0]) == query.format(db_table)

    query = 'CREATE UNIQUE INDEX "index2" ON "{0}" ("other_name") WHERE "name" IS NULL'
    assert str(calls[1]) == query.format(db_table)


def test_cui_upserting():
    """Tests upserting respects the :see:ConditionalUniqueIndex rules."""
    model = get_fake_model(
        fields={
            "a": models.IntegerField(),
            "b": models.IntegerField(null=True),
            "c": models.IntegerField(),
        },
        meta_options={
            "indexes": [
                ConditionalUniqueIndex(
                    fields=["a", "b"], condition='"b" IS NOT NULL'
                ),
                ConditionalUniqueIndex(fields=["a"], condition='"b" IS NULL'),
            ]
        },
    )

    model.objects.upsert(
        conflict_target=["a"],
        index_predicate='"b" IS NULL',
        fields=dict(a=1, c=1),
    )
    assert model.objects.all().count() == 1
    assert model.objects.filter(a=1, c=1).count() == 1

    model.objects.upsert(
        conflict_target=["a"],
        index_predicate='"b" IS NULL',
        fields=dict(a=1, c=2),
    )
    assert model.objects.all().count() == 1
    assert model.objects.filter(a=1, c=1).count() == 0
    assert model.objects.filter(a=1, c=2).count() == 1

    model.objects.upsert(
        conflict_target=["a", "b"],
        index_predicate='"b" IS NOT NULL',
        fields=dict(a=1, b=1, c=1),
    )
    assert model.objects.all().count() == 2
    assert model.objects.filter(a=1, c=2).count() == 1
    assert model.objects.filter(a=1, b=1, c=1).count() == 1

    model.objects.upsert(
        conflict_target=["a", "b"],
        index_predicate='"b" IS NOT NULL',
        fields=dict(a=1, b=1, c=2),
    )
    assert model.objects.all().count() == 2
    assert model.objects.filter(a=1, c=1).count() == 0
    assert model.objects.filter(a=1, b=1, c=2).count() == 1


def test_cui_inserting():
    """Tests inserting respects the :see:ConditionalUniqueIndex rules."""

    model = get_fake_model(
        fields={
            "a": models.IntegerField(),
            "b": models.IntegerField(null=True),
            "c": models.IntegerField(),
        },
        meta_options={
            "indexes": [
                ConditionalUniqueIndex(
                    fields=["a", "b"], condition='"b" IS NOT NULL'
                ),
                ConditionalUniqueIndex(fields=["a"], condition='"b" IS NULL'),
            ]
        },
    )

    model.objects.create(a=1, c=1)
    with transaction.atomic():
        with pytest.raises(IntegrityError):
            model.objects.create(a=1, c=2)
    model.objects.create(a=2, c=1)

    model.objects.create(a=1, b=1, c=1)
    with transaction.atomic():
        with pytest.raises(IntegrityError):
            model.objects.create(a=1, b=1, c=2)

    model.objects.create(a=1, b=2, c=1)
