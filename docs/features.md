# HStoreField
`psqlextra.fields.HStoreField` is based on Django's [HStoreField](https://docs.djangoproject.com/en/1.10/ref/contrib/postgres/fields/#hstorefield) and therefore supports everything Django does natively, plus more.

## uniqueness
The `uniqueness` constraint can be added on one or more `hstore` keys, similar to how a `UNIQUE` constraint can be added to a column. Setting this option causes unique indexes to be created on the specified keys.

You can specify a `list` of strings to specify the keys that must be marked as unique:

    from psqlextra.fields import HStoreField
    from psqlextra.models import PostgresModel

    class MyModel(PostgresModel):
        myfield = HStoreField(uniqueness=['key1']

    MyModel.objects.create(myfield={'key1': 'value1'})
    MyModel.objects.create(myfield={'key1': 'value1'})

The second `create` call will fail with a [IntegrityError](https://docs.djangoproject.com/en/1.10/ref/exceptions/#django.db.IntegrityError) because there's already a row with `key1=value1`.

Uniqueness can also be enforced "together", similar to Django's [unique_together](https://docs.djangoproject.com/en/1.10/ref/models/options/#unique-together) by specifying a tuple of fields rather than a single string:

    myfield = HStoreField(uniqueness=[('key1', 'key2'), 'key3])

In the example above, `key1` and `key2` must unique **together**, and `key3` must unique on its own. By default, none of the keys are marked as "unique".

## required
The `required` option can be added to ensure that the specified `hstore` keys are set for every row. This is similar to a `NOT NULL` constraint on a column. You can specify a list of `hstore` keys that are required:

    from psqlextra.fields import HStoreField
    from psqlextra.models import PostgresModel

    class MyModel(PostgresModel):
        myfield = HStoreField(required=['key1'])

    mymodel.objects.create(myfield={'key1': none})
    MyModel.objects.create(myfield={'key2': 'value1'})

Both calls to `create` would fail in the example above since they do not provide a non-null value for `key1`. By default, none of the keys are required.

# Upsert
An "upsert" is an operation where a piece of data is inserted/created if it doesn't exist yet and updated (overwritten) when it already exists. Django has long provided this functionality through [`update_or_create`](https://docs.djangoproject.com/en/1.10/ref/models/querysets/#update-or-create). It does this by first checking whether the record exists and creating it not.

The major problem with this approach is possibility of race conditions. In between the `SELECT` and `INSERT`, another process could perform the `INSERT`. The last `INSERT` would most likely fail because it would be duplicating a `UNIQUE` constraint.

In order to combat this, PostgreSQL added native upserts. Also known as [`ON CONFLICT DO ...`](https://www.postgresql.org/docs/9.5/static/sql-insert.html#SQL-ON-CONFLICT). This allows a user to specify what to do when a conflict occurs.


## upsert
The `upsert` method attempts to insert a row with the specified data or updates (and overwrites) the duplicate row, and then returns the primary key of the row that was created/updated.

Upserts work by catching conflicts. PostgreSQL requires to know which conflicts to react to. You have to specify the name of the column which's constraint you want to react to. This is specified in the `conflict_target` field. If the constraint you're trying to react to consists of multiple columns, specify multiple columns.

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

## upsert_and_get
`upsert_and_get` does the same thing as `upsert`, but returns a model instance rather than the primary key of the row that was created/updated. This also happens in a single query using `RETURNING` clause on the `INSERT INTO` statement:

    from django.db import models
    from psqlextra.models import PostgresModel

    class MyModel(PostgresModel):
        myfield = models.CharField(max_length=255, unique=True)

    obj1 = MyModel.objects.upsert_and_get(myfield='beer')
    obj2 = MyModel.objects.upsert_and_get(myfield='beer')

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
