.. include:: ./snippets/postgres_doc_links.rst
.. include:: ./snippets/manager_model_warning.rst

.. _hstore_page:

HStore
======

:class:`psqlextra.fields.HStoreField` is based on Django's :class:`~django:django.contrib.postgres.fields.HStoreField` and therefor supports everything Django does natively and more.


Constraints
-----------

Unique
******

The ``uniqueness`` constraint can be added on one or more `hstore`_ keys, similar to how a ``UNIQUE`` constraint can be added to a column. Setting this option causes unique indexes to be created on the specified keys.

You can specify a ``list`` of strings to specify the keys that must be marked as unique:

.. code-block:: python

   from psqlextra.fields import HStoreField
   from psqlextra.models import PostgresModel

   class MyModel(PostgresModel):
       myfield = HStoreField(uniqueness=['key1']

   MyModel.objects.create(myfield={'key1': 'value1'})
   MyModel.objects.create(myfield={'key1': 'value1'})

The second :meth:`~django:django.db.models.query.QuerySet.create` call will fail with a :class:`~django:django.db.IntegrityError` because there's already a row with ``key1=value1``.

Uniqueness can also be enforced "together", similar to Django's :attr:`~django:django.db.models.Options.unique_together` by specifying a tuple of fields rather than a single string:

.. code-block:: python

   myfield = HStoreField(uniqueness=[('key1', 'key2'), 'key3'])

In the example above, ``key1`` and ``key2`` must unique **together**, and ``key3`` must unique on its own. By default, none of the keys are marked as "unique".


Required
********

The ``required`` option can be added to ensure that the specified `hstore`_ keys are set for every row. This is similar to a ``NOT NULL`` constraint on a column. You can specify a list of `hstore`_ keys that are required:

.. code-block:: python

   from psqlextra.fields import HStoreField
   from psqlextra.models import PostgresModel

   class MyModel(PostgresModel):
       myfield = HStoreField(required=['key1'])

       mymodel.objects.create(myfield={'key1': none})
       MyModel.objects.create(myfield={'key2': 'value1'})


Both calls to :meth:`~django:django.db.models.query.QuerySet.create` would fail in the example above since they do not provide a non-null value for ``key1``. By default, none of the keys are required.
