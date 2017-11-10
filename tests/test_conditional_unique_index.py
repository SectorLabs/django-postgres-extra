import pytest

from psqlextra.indexes import ConditionalUniqueIndex
from .migrations import MigrationSimulator

from django.db import models, IntegrityError, transaction
from django.db.migrations import AddIndex, CreateModel


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
