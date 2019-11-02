.. include:: ./snippets/postgres_doc_links.rst

Deletion
========

.. _truncate_page:

Truncate
--------
In standard Django, deleting all records in a table is quite slow and cumbersome. It requires retrieving all rows from the database and deleting them one by one (unless you use bulk delete). Postgres has a standard statement for emptying out a table: `TRUNCATE TABLE`_.

Using the :meth:`~psqlextra.manager.PostgresManager.truncate` method on the :class:`~psqlextra.manager.PostgresManager` allows you to delete all records in a table in the blink of an eye:

.. code-block:: python

   from django.db import models
   from psqlextra.models import PostgresModel

   class MyModel(PostgresModel):
       myfield = models.CharField(max_length=255, unique=True)

   MyModel.objects.create(myfield="1")

   MyModel.objects.truncate() # table is empty after this
   print(MyModel.objects.count()) # zero records left


Cascade
*******

By default, Postgres will raise an error if any other table is referencing one of the rows you're trying to delete. One can tell Postgres to cascade the truncate operation to all related rows.

.. code-block:: python

   from django.db import models
   from psqlextra.models import PostgresModel

   class MyModel1(PostgresModel):
       myfield = models.CharField(max_length=255, unique=True)


   class MyModel2(PostgresModel):
       mymodel1 = models.ForeignKey(Model1, on_delete=models.CASCAD)

   obj1 = MyModel1.objects.create(myfield="1")
   MyModel2.objects.create(mymodel1=obj1)

   MyModel.objects.truncate(cascade=True)
   print(MyModel1.objects.count()) # zero records left
   print(MyModel2.objects.count()) # zero records left
