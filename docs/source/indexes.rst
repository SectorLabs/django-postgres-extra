.. _indexes_page:

Indexes
=======

.. _unique_index_page:

Unique Index
-----------------------------
The :class:`~psqlextra.indexes.UniqueIndex` lets you create a unique index. Normally Django only allows you to create unique indexes by specifying ``unique=True`` on the model field.

Although it can be used on any Django model, it is most useful on views and materialized views where ``unique=True`` does not work.

.. code-block:: python

   from django.db import models
   from psqlextra.indexes import UniqueIndex

   class Model(models.Model):
       class Meta:
           indexes = [
               UniqueIndex(fields=['name']),
           ]

       name = models.CharField(max_length=255)

   Model.objects.create(name='henk')
   Model.objects.create(name='henk') # raises IntegrityError


.. _conditional_unique_index_page:

Conditional Unique Index
------------------------
The :class:`~psqlextra.indexes.ConditionalUniqueIndex` lets you create partial unique indexes in case you ever need :attr:`~django:django.db.models.Options.unique_together` constraints
on nullable columns.

.. warning::

    In Django 3.1 or newer, you might want to use :attr:`~django.db.models.indexes.condition` instead.

Before:

.. code-block:: python

   from django.db import models

   class Model(models.Model):
       class Meta:
           unique_together = ['a', 'b']

       a = models.ForeignKey('some_model', null=True)
       b = models.ForeignKey('some_other_model')

   # Works like a charm!
   b = B()
   Model.objects.create(a=None, b=b)
   Model.objects.create(a=None, b=b)

After:

.. code-block:: python

   from django.db import models
   from psqlextra.indexes import ConditionalUniqueIndex

   class Model(models.Model):
       class Meta:
           indexes = [
               ConditionalUniqueIndex(fields=['a', 'b'], condition='"a" IS NOT NULL'),
               ConditionalUniqueIndex(fields=['b'], condition='"a" IS NULL')
           ]

       a = models.ForeignKey('some_model', null=True)
       b = models.ForeignKey('some_other_model')

   # Integrity Error!
   b = B()
   Model.objects.create(a=None, b=b)
   Model.objects.create(a=None, b=b)

.. _case_insensitive_unique_index_page:

Case Insensitive Unique Index
-----------------------------
The :class:`~psqlextra.indexes.CaseInsensitiveUniqueIndex` lets you create an index that ignores the casing for the specified field(s).

This makes the field(s) behave more like a text field in MySQL.

.. code-block:: python

   from django.db import models
   from psqlextra.indexes import CaseInsensitiveUniqueIndex

   class Model(models.Model):
       class Meta:
           indexes = [
               CaseInsensitiveUniqueIndex(fields=['name']),
           ]

       name = models.CharField(max_length=255)

   Model.objects.create(name='henk')
   Model.objects.create(name='Henk') # raises IntegrityError
