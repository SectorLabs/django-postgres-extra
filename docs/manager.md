# Using the manager
`django-postgres-extra` provides the `psqlextra.manager.PostgresManager` which exposes a lot of functionality. Your model must use this manager in order to use most of this package's functionality.

There's three ways to do this:

* **Inherit your model from `psqlextra.models.PostgresModel`:**

        from psqlextra.models import PostgresModel

        class MyModel(PostgresModel):
            myfield = models.CharField(max_length=255)


* **Override default manager with `psqlextra.manager.PostgresManager`:**

        from django.db import models
        from psqlextra.manager import PostgresManager

        class MyModel(models.Model):
            # override default django manager
            objects = PostgresManager()

            myfield = models.CharField(max_length=255)


* **Provide `psqlextra.manager.PostgresManager` as a custom manager:**

        from django.db import models
        from psqlextra.manager import PostgresManager

        class MyModel(models.Model):
            # custom mananger name
            beer = PostgresManager()

            myfield = models.CharField(max_length=255)

        # use like this:
        MyModel.beer.upsert(..)

        # not like this:
        MyModel.objects.upsert(..) # error!

## Upserting
An "upsert" is an operation where a piece of data is inserted/created if it doesn't exist yet and updated (overwritten) when it already exists. Django has long provided this functionality through [`update_or_create`](https://docs.djangoproject.com/en/1.10/ref/models/querysets/#update-or-create). It does this by first checking whether the record exists and creating it not.

The major problem with this approach is possibility of race conditions. In between the `SELECT` and `INSERT`, another process could perform the `INSERT`. The last `INSERT` would most likely fail because it would be duplicating a `UNIQUE` constraint.

In order to combat this, PostgreSQL added native upserts. Also known as [`ON CONFLICT DO ...`](https://www.postgresql.org/docs/9.5/static/sql-insert.html#SQL-ON-CONFLICT). This allows a user to specify what to do when a conflict occurs.

### upsert
Attempts to insert a row with the specified data or updates (and overwrites) the duplicate row, and then returns the primary key of the row that was created/updated.

Upserts work by catching conflcits. PostgreSQL requires to know whichconflicts to react to. You have to specify the name of the column to which you want to react to. This is specified in the `conflict_target` parameter.

You can only specify a single "constraint" in this field. You **cannot** react to conflicts in multiple fields. This is a limitation by PostgreSQL. Note that this means **single constraint**, not necessarily a single column. A constraint can cover multiple columns.

    from django.db import models
    from psqlextra.models import PostgresModel

    class MyModel(PostgresModel):
        myfield = models.CharField(max_length=255, unique=True)

    id1 = MyModel.objects.upsert(
        conflict_target=['myfield'],
        fields=dict(
            myfield='beer'
        )
    )

    id2 = MyModel.objects.upsert(
        conflict_target=['myfield'],
        fields=dict(
            myfield='beer'
        )
    )

    assert id1 == id2

Note that a single call to `upsert` results in a single `INSERT INTO ... ON CONFLICT DO UPDATE ...`. This fixes the problem outlined earlier about another process doing the `INSERT` in the mean time.

#### unique_together
As mentioned earlier, `conflict_target` expects a single column name, or multiple if the constraint you want to react to spans multiple columns. Django's [unique_together](https://docs.djangoproject.com/en/1.11/ref/models/options/#unique-together) has this. If you want to react to this constraint that covers multiple columns, specify those columns in the `conflict_target` parameter:

    from django.db import models
    from psqlextra.models import PostgresModel

    class MyModel(PostgresModel):
        class Meta:
            unique_together = ('myfield1', 'myfield2')

        myfield1 = models.CharField(max_length=255)
        myfield1 = models.CharField(max_length=255)

    MyModel.objects.upsert(
        conflict_target=['myfield1', 'myfield2'],
        fields=dict(
            myfield1='beer'
            myfield2='moar beer'
        )
    )

#### hstore
You can specify HStore keys that have a unique constraint as a `conflict_target`:

    from django.db import models
    from psqlextra.models import PostgresModel
    from psqlextra.fields import HStoreField

    class MyModel(PostgresModel):
        # values in the key 'en' have to be unique
        myfield = HStoreField(uniqueness=['en'])

    MyModel.objects.upsert(
        conflict_target=[('myfield', 'en')],
        fields=dict(
            myfield={'en': 'beer'}
        )
    )

It also supports specifying a "unique together" constraint on HStore keys:

    from django.db import models
    from psqlextra.models import PostgresModel
    from psqlextra.fields import HStoreField

    class MyModel(PostgresModel):
        # values in the key 'en' and 'ar' have to be
        # unique together
        myfield = HStoreField(uniqueness=[('en', 'ar')])

    MyModel.objects.upsert(
        conflict_target=[('myfield', 'en'), ('myfield', 'ar')],
        fields=dict(
            myfield={'en': 'beer', 'ar': 'arabic beer'}
        )
    )

### upsert_and_get
Does the same thing as `upsert`, but returns a model instance rather than the primary key of the row that was created/updated. This also happens in a single query using `RETURNING` clause on the `INSERT INTO` statement:

    from django.db import models
    from psqlextra.models import PostgresModel

    class MyModel(PostgresModel):
        myfield = models.CharField(max_length=255, unique=True)

    obj1 = MyModel.objects.create(myfield='beer')
    obj2 = MyModel.objects.create(myfield='beer')

    obj1 = MyModel.objects.upsert_and_get(
        conflict_target=['myfield'],
        fields=dict(
            myfield='beer'
        )
    )

    obj2 = MyModel.objects.upsert_and_get(
        conflict_target=['myfield'],
        fields=dict(
            myfield='beer'
        )
    )

    assert obj1.id == obj2.id
