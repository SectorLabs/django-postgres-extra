# Using the manager
`django-postgres-extra` provides the `psqlextra.manager.PostgresManager` which exposes a lot of functionality. Your model must use this manager in order to use most of this package's functionality.

There are four ways to do this:

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

* **Use the `psqlextra.util.postgres_manager` on the fly:**

    This allows the manager to be used **anywhere** on **any** model, but only within the context. This is especially useful if you want to do upserts into Django's `ManyToManyField`'s generated `through` table:

        from django.db import models
        from psqlextra.util import postgres_manager

        class MyModel(models.Model):
            myself = models.ManyToManyField('self')

        # within the context, you can access psqlextra features
        with postgres_manager(MyModel.myself.through) as manager:
            manager.upsert(...)

## Conflict handling
The `PostgresManager` comes with full support for PostgreSQL's `ON CONFLICT DO ...`. This is an extremely useful feature for doing concurrency safe inserts. Often, when you want to insert a row, you want to overwrite it already exists, or simply leave the existing data there. This would require a `SELECT` first and then possibly a `INSERT`. Within those two queries, another process might make a change to the row. The alternative of trying to insert, ignoring the error and then doing a `UPDATE` is also not good. That would result in a a lot of write overhead (due to logging). Luckily, PostgreSQL offers `ON CONFLICT DO ...`, which allows you to specify what PostgreSQL should do in case that row already exists.

`django-postgres-extra` brings full support for PostgreSQL's `ON CONFLICT DO ...`, allowing blazing fast and concurrency safe inserts:

    from django.db import models
    from psqlextra.models import PostgresModel
    from psqlextra.query import ConflictAction

    class MyModel(PostgresModel):
        myfield = models.CharField(max_length=255, unique=True)

    # insert or update if already exists, then fetch, all in a single query
    obj2 = (
        MyModel.objects
        .on_conflict(['myfield'], ConflictAction.UPDATE)
        .insert_and_get(myfield='beer')
    )

    # insert, or do nothing if it already exists, then fetch
    obj1 = (
        MyModel.objects
        .on_conflict(['myfield'], ConflictAction.NOTHING)
        .insert_and_get(myfield='beer')
    )

    # insert or update if already exists, then fetch only the primary key
    id = (
        MyModel.objects
        .on_conflict(['myfield'], ConflictAction.UPDATE)
        .insert(myfield='beer')
    )

### Constraint specification
The `on_conflict` function's first parameter denotes the name of the column(s) in which the conflict might occur. Although you can specify multiple columns, these columns must somehow have a single constraint. For example, in case of a `unique_together` constraint.

#### Multiple columns
Specifying multiple columns is necessary in case of a constraint that spans multiple columns, such as when using Django's [unique_together](https://docs.djangoproject.com/en/1.11/ref/models/options/#unique-together):

    from django.db import models
    from psqlextra.models import PostgresModel

    class MyModel(PostgresModel)
        class Meta:
            unique_together = ('first_name', 'last_name',)

        first_name = models.CharField(max_length=255)
        last_name = models.CharField(max_length=255)

    obj = (
        MyModel.objects
        .on_conflict(['first_name', 'last_name'], ConflictAction.UPDATE)
        .insert_and_get(first_name='Henk', last_name='Jansen')
    )

#### HStore keys
Catching conflicts in columns with a `UNIQUE` constraint on a `hstore` key is also supported:

    from django.db import models
    from psqlextra.models import PostgresModel
    from psqlextra.fields import HStoreField

    class MyModel(PostgresModel)
        name = HStoreField(uniqueness=['en'])

    id = (
        MyModel.objects
        .on_conflict([('name', 'en')], ConflictAction.NOTHING)
        .insert(name={'en': 'Swen'})
    )

This also applies to "unique together" constraints in a `hstore` field:

    class MyModel(PostgresModel)
        name = HStoreField(uniqueness=[('en', 'ar')])

    id = (
        MyModel.objects
        .on_conflict([('name', 'en'), ('name', 'ar')], ConflictAction.NOTHING)
        .insert(name={'en': 'Swen'})
    )

### insert vs insert_and_get
After specifying `on_conflict` you can use either `insert` or `insert_and_get` to perform the insert.

#### insert
* Perform the insert, and then returns the primary key of the row that was inserted or it conflicted with.

#### insert_and_get
* Perform the insert, then returns the entire row that was inserted or it conflicted with, in the form of a model instance.

### Pitfalls
The standard Django methods for inserting/updating are not affected by `on_conflict`. It was a conscious decision to not override or change their behavior. **The following completely ignores the `on_conflict` **:

    obj = (
        MyModel.objects
        .on_conflict(['first_name', 'last_name'], ConflictAction.UPDATE)
        .create(first_name='Henk', last_name='Jansen')

The same applies to methods such as `update`, `get_or_create`, `update_or_create` etc.

### Conflict actions
There's currently two actions that can be taken when encountering a conflict. The second parameter of `on_conflict` allows you to specify that should happen.

#### ConflictAction.UPDATE
* If the row does **not exist**, insert a new one.
* If the row **exists**, update it.

This is also known as a "upsert".

#### ConflictAction.NOTHING
* If the row does **not exist**, insert a new one.
* If the row **exists**, do nothing.

This is preferable when the data you're about to insert is the same as the one that already exists. This is more performant because it avoids a write in case the row already exists.

### Shorthand
The `on_conflict`, `insert` and `insert_or_create` methods were only added in `django-postgres-extra` 1.6. Before that, only `ConflictAction.UPDATE` was supported in the following form:

    from django.db import models
    from psqlextra.models import PostgresModel

    class MyModel(PostgresModel):
        myfield = models.CharField(max_length=255, unique=True)

    obj = (
        MyModel.objects
        .upsert_and_get(
            conflict_target=['myfield']
            fields=dict(myfield='beer')
        )
    )

    id = (
        MyModel.objects
        .upsert(
            conflict_target=['myfield']
            fields=dict(myfield='beer')
        )
    )

These two short hands still exist and **are not** deprecated. They behave exactly the same as `ConflictAction.UPDATE` and are there for convenience. It is up to you to decide what to use.
