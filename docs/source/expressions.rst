.. include:: ./snippets/postgres_doc_links.rst
.. include:: ./snippets/manager_model_warning.rst

.. _expressions_page:

Expressions
===========


Selecting an individual HStore key
----------------------------------

Use the :class:`~psqlextra.expressions.HStoreRef` expression to select an indvidiual `hstore`_ key:

.. code-block:: python

   from psqlextra.models import PostgresModel
   from psqlextra.fields import HStoreField
   from psqlextra.expressions import HStoreRef

   class MyModel(PostgresModel):
      bla = HStoreField()

   MyModel.objects.create(bla={'a': '1', 'b': '2'})

   # '1'
   a = (
      MyModel.objects
      .annotate(a=HStoreRef('bla', 'a'))
      .values_list('a', flat=True)
      .first()
   )


Selecting a datetime as a UNIX epoch timestamp
----------------------------------------------

Use the :class:`~psqlextra.expressions.DateTimeEpoch` expression to select the value of a :class:`~django:django.db.models.DateTimeField` as a UNIX epoch timestamp.

.. code-block:: python

   from psqlextra.models import PostgresModel
   from psqlextra.fields import HStoreField
   from psqlextra.expressions import DateTimeEpoch

   class MyModel(PostgresModel):
      datetime = DateTimeField(auto_now_add=True)

   MyModel.objects.create()

   timestamp = (
      MyModel.objects
      .annotate(timestamp=DateTimeEpoch('datetime'))
      .values_list('timestamp', flat=True)
      .first()
   )


Multi-field coalesce
--------------------

Use the :class:`~psqlextra.expressions.IsNotNone` expression to perform something similar to a `coalesce`, but with multiple fields. The first non-null value encountered is selected.

.. code-block:: python

   from psqlextra.models import PostgresModel
   from psqlextra.fields import HStoreField
   from psqlextra.expressions import IsNotNone

   class MyModel(PostgresModel):
      name_1 = models.TextField(null=True)
      name_2 = models.TextField(null=True)
      name_3 = models.TextField(null=True)

   MyModel.objects.create(name_3='test')

   # 'test'
   name = (
      MyModel.objects
      .annotate(name=IsNotNone('name_1', 'name_2', 'name_3', default='buh'))
      .values_list('name', flat=True)
      .first()
   )

   # 'buh'
   name = (
      MyModel.objects
      .annotate(name=IsNotNone('name_1', 'name_2', default='buh'))
      .values_list('name', flat=True)
      .first()
   )


Excluded column
---------------

Use the :class:`~psqlextra.expressions.ExcludedCol` expression when performing an upsert using `ON CONFLICT`_ to refer to a column/field in the data is about to be upserted.

PostgreSQL keeps that data to be upserted in a special table named `EXCLUDED`. This expression is used to refer to a column in that table.

.. code-block:: python

    from django.db.models import Q
    from psqlextra.expressions import ExcludedCol

    (
        MyModel
        .objects
        .on_conflict(
            ['name'],
            ConflictAction.UPDATE,
            # translates to `priority > EXCLUDED.priority`
            update_condition=Q(priority__gt=ExcludedCol('priority')),
        )
        .insert(
            name='henk',
            priority=1,
        )
    )
