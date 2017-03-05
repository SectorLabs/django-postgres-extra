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

# Signals
Django has long supported "signals", a feature that can be really useful. It allows you to get notified in a callback when a certain event occurs. One of the most common use cases is connecting to model signals. These signals get triggered when a model gets saved, or deleted.

Django's built-in signals have one major flaw:

* The `QuerySet.update(..)` method does **not** emit **any** signals.

Because of this limitation, Django's signals cannot be reliably used to be signaled on model changes. `django-postges-extra` adds three new signals which are much more primitive, but work reliably across the board.

The signals defined by this library are completely valid, standard Django signals. Therefore, their documentation also applies: [https://docs.djangoproject.com/en/1.10/topics/signals/](https://docs.djangoproject.com/en/1.10/topics/signals/).

Each of the signals send upon model modification send one parameter containing the value of the primary key of the row that was affected. Therefore the signal's signature looks like this:

    def my_receiver(sender, pk: int):
        # pk is the primary key, a keyword argument

* `psqlextra.signals.create`
    * Send **after** a new model instance was created.
        
            from django.db import models
            from psqlextra.models import PostgresModel
            from psqlextra import signals
    
            class MyModel(PostgresModel):
                myfield = models.CharField(max_length=255, unique=True)
                
            def on_create(sender, **kwargs):
                 print('model created with pk %d' % kwargs['pk'])
                
            signals.create.connect(MyModel, on_create, weak=False)

            # this will trigger the signal
            instance = MyModel(myfield='cookies')
            instance.save()
            
            # but so will this
            MyModel.objects.create(myfield='cheers')

* `psqlextra.signals.update`
    * Send **after** a new model instance was updated.
        
            from django.db import models
            from psqlextra.models import PostgresModel
            from psqlextra import signals
    
            class MyModel(PostgresModel):
                myfield = models.CharField(max_length=255, unique=True)
                
            def on_update(sender, **kwargs):
                 print('model updated with pk %d' % kwargs['pk'])
                
            signals.update.connect(MyModel, on_update, weak=False)

            # for every row that is affected, the signal will be send
            MyModel.objects.filter(myfield='cookies').update(myfield='cheers')
            
* `psqlextra.signals.delete`
    * Send **before** a new model instance is deleted.
        
            from django.db import models
            from psqlextra.models import PostgresModel
            from psqlextra import signals
    
            class MyModel(PostgresModel):
                myfield = models.CharField(max_length=255, unique=True)
                
            def on_delete(sender, **kwargs):
                 print('model deleted with pk %d' % kwargs['pk'])
                
            signals.delete.connect(MyModel, on_update, weak=False)

            # for every row that is affected, the signal will be send
            MyModel.objects.filter(myfield='cookies').delete()
            
            # in this case, a single row is deleted, the signal will be send
            # for this particular row
            MyModel.objects.get(id=1).delete()
