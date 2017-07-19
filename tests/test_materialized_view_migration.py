from django.db import migrations
from django.db.models import F
from django.db import models

from psqlextra.models import PostgresMaterializedViewModel
from psqlextra.manager import PostgresMaterializedViewManager

from .migrations import MigrationSimulator


def test_materialized_view_create_migration():
    simulator = MigrationSimulator()

    Model = simulator.define_model({
        'name': models.CharField(max_length=255)
    })

    simulator.make_migrations()
    simulator.migrate()

    Model.objects.create(name='swen')

    MaterializedViewModel = simulator.define_model(
        {
            'id': models.IntegerField(primary_key=True),
            'name': models.CharField(max_length=255),
        },
        PostgresMaterializedViewModel,
        {
            'view_query': Model.objects.annotate(name=F('name')).values('id', 'name')
        }
    )

    migration = simulator.make_migrations()
    assert len(migration.operations) == 1

    operation = migration.operations[0]
    assert isinstance(operation, migrations.CreateModel)

    assert isinstance(
        next(manager for name, manager in operation.managers if name == 'view'),
        PostgresMaterializedViewManager
    )

    assert 'view_query' in operation.options

    simulator.migrate()

    row = MaterializedViewModel.objects.first()
    assert row is not None
    assert row.name == 'swen'
