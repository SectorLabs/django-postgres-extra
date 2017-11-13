import pytest

from psqlextra.indexes import ConditionalUniqueIndex
from .migrations import MigrationSimulator

from django.db import models, IntegrityError, transaction
from django.db.migrations import AddIndex, CreateModel

from .util import get_fake_model


def test_deconstruct():
    """Tests whether the :see:HStoreField's deconstruct()
    method works properly."""

    original_kwargs = dict(condition='field IS NULL', name='great_index', fields=['field', 'build'])
    _, _, new_kwargs = ConditionalUniqueIndex(**original_kwargs).deconstruct()

    for key, value in original_kwargs.items():
        assert new_kwargs[key] == value


def test_migrations():
    """Tests whether the migrations are properly generated and executed."""

    simulator = MigrationSimulator()

    Model = simulator.define_model(
        fields={
            'id': models.IntegerField(primary_key=True),
            'name': models.CharField(max_length=255, null=True),
            'other_name': models.CharField(max_length=255)
        },
        meta_options={
            'indexes': [
                ConditionalUniqueIndex(
                    fields=['name', 'other_name'],
                    condition='"name" IS NOT NULL',
                    name='index1'
                ),
                ConditionalUniqueIndex(
                    fields=['other_name'],
                    condition='"name" IS NULL',
                    name='index2'
                )
            ]
        }
    )

    migration = simulator.make_migrations()
    assert len(migration.operations) == 3

    operations = migration.operations
    assert isinstance(operations[0], CreateModel)

    for operation in operations[1:]:
        assert isinstance(operation, AddIndex)

    calls = [call[0] for _, call, _ in simulator.migrate('CREATE UNIQUE INDEX')[0]['CREATE UNIQUE INDEX']]

    db_table = Model._meta.db_table
    assert calls[0] == 'CREATE UNIQUE INDEX "index1" ON "{0}" ("name", "other_name") WHERE "name" IS NOT NULL'.format(
        db_table
    )
    assert calls[1] == 'CREATE UNIQUE INDEX "index2" ON "{0}" ("other_name") WHERE "name" IS NULL'.format(
        db_table
    )

    with transaction.atomic():
        Model.objects.create(id=1, name="name", other_name="other_name")
        with pytest.raises(IntegrityError):
            Model.objects.create(id=2, name="name", other_name="other_name")

    with transaction.atomic():
        Model.objects.create(id=1, name=None, other_name="other_name")
        with pytest.raises(IntegrityError):
            Model.objects.create(id=2, name=None, other_name="other_name")


def test_upserting():
    """Tests upserting respects the :see:ConditionalUniqueIndex rules"""
    model = get_fake_model(
        fields={
            'a': models.IntegerField(),
            'b': models.IntegerField(null=True),
            'c': models.IntegerField(),
        },
        meta_options={
            'indexes': [
                ConditionalUniqueIndex(
                    fields=['a', 'b'],
                    condition='"b" IS NOT NULL'
                ),
                ConditionalUniqueIndex(
                    fields=['a'],
                    condition='"b" IS NULL'
                )
            ]
        }
    )

    model.objects.upsert(conflict_target=['a'], index_predicate='"b" IS NULL', fields=dict(a=1, c=1))
    assert model.objects.all().count() == 1
    assert model.objects.filter(a=1, c=1).count() == 1

    model.objects.upsert(conflict_target=['a'], index_predicate='"b" IS NULL', fields=dict(a=1, c=2))
    assert model.objects.all().count() == 1
    assert model.objects.filter(a=1, c=1).count() == 0
    assert model.objects.filter(a=1, c=2).count() == 1

    model.objects.upsert(conflict_target=['a', 'b'], index_predicate='"b" IS NOT NULL', fields=dict(a=1, b=1, c=1))
    assert model.objects.all().count() == 2
    assert model.objects.filter(a=1, c=2).count() == 1
    assert model.objects.filter(a=1, b=1, c=1).count() == 1

    model.objects.upsert(conflict_target=['a', 'b'], index_predicate='"b" IS NOT NULL', fields=dict(a=1, b=1, c=2))
    assert model.objects.all().count() == 2
    assert model.objects.filter(a=1, c=1).count() == 0
    assert model.objects.filter(a=1, b=1, c=2).count() == 1


def test_inserting():
    """Tests inserting respects the :see:ConditionalUniqueIndex rules"""

    model = get_fake_model(
        fields={
            'a': models.IntegerField(),
            'b': models.IntegerField(null=True),
            'c': models.IntegerField(),
        },
        meta_options={
            'indexes': [
                ConditionalUniqueIndex(
                    fields=['a', 'b'],
                    condition='"b" IS NOT NULL'
                ),
                ConditionalUniqueIndex(
                    fields=['a'],
                    condition='"b" IS NULL'
                )
            ]
        }
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
