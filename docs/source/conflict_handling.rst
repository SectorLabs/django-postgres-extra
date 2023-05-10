.. include:: ./snippets/postgres_doc_links.rst
.. include:: ./snippets/manager_model_warning.rst

.. _conflict_handling_page:

Conflict handling
=================

The :class:`~psqlextra.manager.PostgresManager` comes with full support for PostgreSQL's `ON CONFLICT`_ clause.

This is an extremely useful feature for doing concurrency safe inserts. Often, when you want to insert a row, you want to overwrite it already exists, or simply leave the existing data there. This would require a ``SELECT`` first and then possibly a ``INSERT``. Within those two queries, another process might make a change to the row.

The alternative of trying to insert, ignoring the error and then doing a ``UPDATE`` is also not good. That would result in a lot of write overhead (due to logging).

.. code-block:: python

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

.. warning::

   The standard Django methods for inserting/updating are not affected by :meth:`~psqlextra.query.PostgresQuerySet.on_conflict`. It was a conscious decision to not override or change their behavior. The following completely ignores the :meth:`~psqlextra.query.PostgresQuerySet.on_conflict`:

   .. code-block:: python

      obj = (
          MyModel.objects
          .on_conflict(['first_name', 'last_name'], ConflictAction.UPDATE)
          .create(first_name='Henk', last_name='Jansen')
      )

   The same applies to methods such as :meth:`~django:django.db.models.query.QuerySet.update`, :meth:`~django:django.db.models.query.QuerySet.get_or_create` or :meth:`~django:django.db.models.query.QuerySet.update_or_create` etc.


Constraint specification
------------------------

The :meth:`~psqlextra.query.PostgresQuerySet.on_conflict` function's first parameter denotes the name of the column(s) in which the conflict might occur. Although you can specify multiple columns, these columns must somehow have a single constraint. For example, in case of a :attr:`~django:django.db.models.Options.unique_together` constraint.


Multiple columns
****************

Specifying multiple columns is necessary in case of a constraint that spans multiple columns, such as when using Django's :attr:`~django:django.db.models.Options.unique_together`.

.. code-block:: python

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


Specific constraint
*******************

Alternatively, instead of specifying the columns the constraint you're targetting applies to, you can also specify the exact constraint to use:

.. code-block:: python

   from django.db import models
   from psqlextra.models import PostgresModel

   class MyModel(PostgresModel)
       class Meta:
           constraints = [
               models.UniqueConstraint(
                   name="myconstraint",
                   fields=["first_name", "last_name"]
               ),
           ]

       first_name = models.CharField(max_length=255)
       last_name = models.CharField(max_length=255)

   constraint = next(
       constraint
       for constraint in MyModel._meta.constraints
       if constraint.name == "myconstraint"
    ), None)

   obj = (
       MyModel.objects
       .on_conflict(constraint, ConflictAction.UPDATE)
       .insert_and_get(first_name='Henk', last_name='Jansen')
   )


HStore keys
***********
Catching conflicts in columns with a ``UNIQUE`` constraint on a :class:`~psqlextra.fields.HStoreField` key is also supported:

.. code-block:: python

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

This also applies to "unique together" constraints in a :class:`~psqlextra.fields.HStoreField` field:

.. code-block:: python

   class MyModel(PostgresModel)
       name = HStoreField(uniqueness=[('en', 'ar')])

   id = (
       MyModel.objects
       .on_conflict([('name', 'en'), ('name', 'ar')], ConflictAction.NOTHING)
       .insert(name={'en': 'Swen'})
   )


insert vs insert_and_get
------------------------

After specifying :meth:`~psqlextra.query.PostgresQuerySet.on_conflict` you can use either :meth:`~psqlextra.query.PostgresQuerySet.insert` or :meth:`~psqlextra.query.PostgresQuerySet.insert_and_get` to perform the insert.


Conflict actions
----------------
There's currently two actions that can be taken when encountering a conflict. The second parameter of :meth:`~psqlextra.query.PostgresQuerySet.on_conflict` allows you to specify that should happen.


ConflictAction.UPDATE
*********************

:attr:`psqlextra.types.ConflictAction.UPDATE`

* If the row does **not exist**, insert a new one.
* If the row **exists**, update it.

This is also known as a "upsert".

Condition
"""""""""

Optionally, a condition can be added. PostgreSQL will then only apply the update if the condition holds true. A condition is specified as a custom expression.

A row level lock is acquired before evaluating the condition and proceeding with the update.

.. note::

    The update condition is translated as a condition for `ON CONFLICT`_. The PostgreSQL documentation states the following:

        An expression that returns a value of type boolean. Only rows for which this expression returns true will be updated, although all rows will be locked when the ON CONFLICT DO UPDATE action is taken. Note that condition is evaluated last, after a conflict has been identified as a candidate to update.


.. code-block:: python

    from psqlextra.expressions import CombinedExpression, ExcludedCol

    pk = (
        MyModel
        .objects
        .on_conflict(
            ['name'],
            ConflictAction.UPDATE,
            update_condition=CombinedExpression(
                MyModel._meta.get_field('priority').get_col(MyModel._meta.db_table),
                '>',
                ExcludedCol('priority'),
            )
        )
        .insert(
            name='henk',
            priority=1,
        )
    )

    if pk:
        print('update applied or inserted')
    else:
        print('condition was false-ish and no changes were made')


When writing expressions, refer to the data you're trying to upsert with the :class:`psqlextra.expressions.ExcludedCol` expression.

Alternatively, with Django 3.1 or newer, :class:`~django:django.db.models.Q` objects can be used instead:

.. code-block:: python

    from django.db.models import Q
    from psqlextra.expressions import ExcludedCol

    Q(name=ExcludedCol('name'))
    Q(name__isnull=True)
    Q(name__gt=ExcludedCol('priority'))


ConflictAction.NOTHING
**********************

:attr:`psqlextra.types.ConflictAction.NOTHING`

* If the row does **not exist**, insert a new one.
* If the row **exists**, do nothing.

This is preferable when the data you're about to insert is the same as the one that already exists. This is more performant because it avoids a write in case the row already exists.

.. warning::

   When using :attr:`~psqlextra.types.ConflictAction.NOTHING`, PostgreSQL only returns the row(s) that were created. Conflicting rows are not returned. See example below:

   .. code-block:: python

      # obj1 is _not_ none
      obj1 = MyModel.objects.on_conflict(['name'], ConflictAction.NOTHING).insert(name="me")

      # obj2 is none! object alreaddy exists
      obj2 = MyModel.objects.on_conflict(['name'], ConflictAction.NOTHING).insert(name="me")

   This applies all methods: :meth:`~psqlextra.query.PostgresQuerySet.insert`, :meth:`~psqlextra.query.PostgresQuerySet.insert_and_get`, :meth:`~psqlextra.query.PostgresQuerySet.bulk_insert`


Bulk
----

:meth:`~psqlextra.query.PostgresQuerySet.bulk_insert` allows your to use conflict resolution for bulk inserts:

.. code-block:: python

   from django.db import models
   from psqlextra.models import PostgresModel

   class MyModel(PostgresModel):
       name = models.CharField(max_length=255, unique=True)

   obj = (
       MyModel.objects
       .on_conflict(['name'], ConflictAction.UPDATE)
       .bulk_insert([
           dict(name='swen'),
           dict(name='henk'),
           dict(name='adela')
       ])
   )

:meth:`~psqlextra.query.PostgresQuerySet.bulk_insert` uses a single query to insert all specified rows at once. It returns a ``list`` of ``dict`` with each ``dict`` being a merge of the ``dict`` passed in along with any index returned from Postgres.

.. note::

   In order to stick to the "everything in one query" principle, various, more advanced usages of :meth:`~psqlextra.query.PostgresQuerySet.bulk_insert` are impossible. It is not possible to have different rows specify different amounts of columns. The following example does **not work**:

   .. code-block:: python

      from django.db import models
      from psqlextra.models import PostgresModel

      class MyModel(PostgresModel):
          first_name = models.CharField(max_length=255, unique=True)
          last_name = models.CharField(max_length=255, default='kooij')

      obj = (
          MyModel.objects
          .on_conflict(['name'], ConflictAction.UPDATE)
          .bulk_insert([
              dict(name='swen'),
              dict(name='henk', last_name='poepjes'), # invalid, different column configuration
              dict(name='adela')
          ])
      )

   An exception is thrown if this behavior is detected.


Shorthands
----------

The :meth:`~psqlextra.query.PostgresQuerySet.on_conflict`, :meth:`~psqlextra.query.PostgresQuerySet.insert` and :meth:`~psqlextra.query.PostgresQuerySet.insert_or_create` methods were only added in v1.6. Before that, only :attr:`~psqlextra.types.ConflictAction.UPDATE` was supported in the following form:

.. code-block:: python

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

   (
       MyModel.objects
       .bulk_upsert(
           conflict_target=['myfield']
           rows=[
               dict(myfield='beer'),
               dict(myfield='wine')
           ]
       )
   )

These two short hands still exist and **are not** deprecated. They behave exactly the same as :attr:`~psqlextra.types.ConflictAction.UPDATE` and are there for convenience. It is up to you to decide what to use.
