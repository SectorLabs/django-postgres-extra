.. include:: ./snippets/postgres_doc_links.rst

.. warning::

   Table partitioning is a relatively new and advanded PostgreSQL feature. It has plenty of ways to shoot yourself in the foot with.

   We HIGHLY RECOMMEND you only use this feature if you're already deeply familiar with table partitioning and aware of its advantages and disadvantages.

   Do study the PostgreSQL documentation carefully.

.. _table_partitioning_page:


Table partitioning
==================

:class:`~psqlextra.models.PostgresPartitionedModel` adds support for `PostgreSQL Declarative Table Partitioning`_.

The following partitioning methods are available:

* ``PARTITION BY RANGE``
* ``PARTITION BY LIST``

.. note::

   Although table partitioning is available in PostgreSQL 10.x, it is highly recommended you use PostgresSQL 11.x. Table partitioning got a major upgrade in PostgreSQL 11.x.

   PostgreSQL 10.x does not support creating foreign keys to/from partitioned tables and does not automatically create an index across all partitions.


Creating partitioned tables
---------------------------

Partitioned tables are declared like regular Django models with a special base class and two extra options to set the partitioning method and key. Once declared, they behave like regular Django models.


Declaring the model
*******************

Inherit your model from :class:`psqlextra.models.PostgresPartitionedModel` and declare a child class named ``PartitioningMeta``. On the meta class, specify the partitioning method and key.

* Use :attr:`psqlextra.types.PostgresPartitioningMethod.RANGE` to ``PARTITION BY RANGE``
* Use :attr:`psqlextra.types.PostgresPartitioningMethod.LIST` to ``PARTITION BY LIST``

.. code-block:: python

   from django.db import models

   from psqlextra.types import PostgresPartitioningMethod
   from psqlextra.models import PostgresPartitionedModel

   class MyModel(PostgresPartitionedModel):
       class PartitioningMeta:
           method = PostgresPartitioningMethod.RANGE
           key = ["timestamp"]

       name = models.TextField()
       timestamp = models.DateTimeField()


Generating a migration
**********************
Run the following command to automatically generate a migration:

.. code-block:: bash

   python manage.py pgmakemigrations

This will generate a migration that creates the partitioned table with a default partition.


.. warning::

    Always use ``python manage.py pgmakemigrations`` for partitioned models.

    The model must be created by the :class:`~psqlextra.backend.migrations.operations.PostgresCreatePartitionedModel` operation.

    Do not use the standard ``python manage.py makemigrations`` command for partitioned models. Django will issue a standard :class:`~django:django.db.migrations.operations.CreateModel` operation. Doing this will not create a partitioned table and all subsequent operations will fail.


Adding/removing partitions manually
-----------------------------------

Postgres does not have support for automatically creating new partitions as needed. Therefore, one must manually add new partitions. Depending on the partitioning method you have chosen, the partition has to be created differently.

Partitions are tables. Each partition must be given a unique name. :class:`~psqlextra.models.PostgresPartitionedModel` does not require you to create a model for each partition because you are not supposed to query partitions directly.


Adding a range partition
~~~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`~psqlextra.backend.migrations.operations.PostgresAddRangePartition` operation to add a new range partition. Only use this operation when your partitioned model uses the :attr:`psqlextra.types.PostgresPartitioningMethod.RANGE`.

.. code-block:: python

   from django.db import migrations, models

   from psqlextra.backend.migrations.operations import PostgresAddRangePartition

   class Migration(migrations.Migration):
       operations = [
           PostgresAddRangePartition(
              model_name="mypartitionedmodel",
              name="pt1",
              from_values="2019-01-01",
              to_values="2019-02-01",
           ),
       ]


Adding a list partition
~~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`~psqlextra.backend.migrations.operations.PostgresAddListPartition` operation to add a new list partition. Only use this operation when your partitioned model uses the :attr:`psqlextra.types.PostgresPartitioningMethod.LIST`.

.. code-block:: python

   from django.db import migrations, models

   from psqlextra.backend.migrations.operations import PostgresAddListPartition

   class Migration(migrations.Migration):
       operations = [
           PostgresAddListPartition(
              model_name="mypartitionedmodel",
              name="pt1",
              values=["car", "boat"],
           ),
       ]


Adding a default partition
~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`~psqlextra.backend.migrations.operations.PostgresAddDefaultPartition` operation to add a new default partition. A default partition is the partition where records get saved that couldn't fit in any other partition.

Note that you can only have one default partition per partitioned table/model.

.. code-block:: python

   from django.db import migrations, models

   from psqlextra.backend.migrations.operations import PostgresAddDefaultPartition

   class Migration(migrations.Migration):
       operations = [
           PostgresAddDefaultPartition(
              model_name="mypartitionedmodel",
              name="default",
           ),
       ]


Deleting a default partition
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`~psqlextra.backend.migrations.operations.PostgresDeleteDefaultPartition` operation to delete an existing default partition.

.. code-block:: python

   from django.db import migrations, models

   from psqlextra.backend.migrations.operations import PostgresDeleteDefaultPartition

   class Migration(migrations.Migration):
       operations = [
           PostgresDeleteDefaultPartition(
              model_name="mypartitionedmodel",
              name="pt1",
           ),
       ]


Deleting a range partition
~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`psqlextra.backend.migrations.operations.PostgresDeleteRangePartition` operation to delete an existing range partition.

.. code-block:: python

   from django.db import migrations, models

   from psqlextra.backend.migrations.operations import PostgresDeleteRangePartition

   class Migration(migrations.Migration):
       operations = [
           PostgresDeleteRangePartition(
              model_name="mypartitionedmodel",
              name="pt1",
           ),
       ]


Deleting a list partition
~~~~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`~psqlextra.backend.migrations.operations.PostgresDeleteListPartition` operation to delete an existing list partition.

.. code-block:: python

   from django.db import migrations, models

   from psqlextra.backend.migrations.operations import PostgresDeleteListPartition

   class Migration(migrations.Migration):
       operations = [
           PostgresDeleteListPartition(
              model_name="mypartitionedmodel",
              name="pt1",
           ),
       ]


Using the schema editor
***********************

Use the :class:`psqlextra.backend.PostgresSchemaEditor` to manage partitions directly in a more imperative fashion. The schema editor is used by the migration operations described above.


Adding a range partition
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from django.db import connection

   connection.schema_editor().add_range_partition(
       model=MyPartitionedModel,
       name="pt1",
       from_values="2019-01-01",
       to_values="2019-02-01",
   )


Adding a list partition
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from django.db import connection

   connection.schema_editor().add_list_partition(
       model=MyPartitionedModel,
       name="pt1",
       values=["car", "boat"],
   )


Adding a default partition
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from django.db import connection

   connection.schema_editor().add_default_partition(
       model=MyPartitionedModel,
       name="default",
   )


Deleting a partition
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from django.db import connection

   connection.schema_editor().delete_partition(
       model=MyPartitionedModel,
       name="default",
   )


Adding/removing partitions automatically
----------------------------------------

:class:`psqlextra.partitioning.PostgresPartitioningManager` an experimental helper class that can be called periodically to automatically create new partitions if you're using range partitioning.

.. note::

   There is currently no scheduler or command to automatically create new partitions. You'll have to run this function in your own cron jobs.

The auto partitioner supports automatically creating yearly, monthly, weekly or daily partitions. Use the ``count`` parameter to configure how many partitions it should create ahead.


Partitioning strategies
***********************


Time-based partitioning
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from dateutil.relativedelta import relativedelta

   from psqlextra.partitioning import (
       PostgresPartitioningManager,
       PostgresCurrentTimePartitioningStrategy,
       PostgresTimePartitionSize,
       partition_by_current_time,
   )

   manager = PostgresPartitioningManager([
       # 3 partitions ahead, each partition is one month
       # delete partitions older than 6 months
       # partitions will be named `[table_name]_[year]_[3-letter month name]`.
       PostgresPartitioningConfig(
           model=MyPartitionedModel,
           strategy=PostgresCurrentTimePartitioningStrategy(
               size=PostgresTimePartitionSize(months=1),
               count=3,
               max_age=relativedelta(months=6),
           ),
       ),
       # 6 partitions ahead, each partition is two weeks
       # delete partitions older than 8 months
       # partitions will be named `[table_name]_[year]_week_[week number]`.
       PostgresPartitioningConfig(
           model=MyPartitionedModel,
           strategy=PostgresCurrentTimePartitioningStrategy(
               size=PostgresTimePartitionSize(weeks=2),
               count=6,
               max_age=relativedelta(months=8),
           ),
       ),
       # 12 partitions ahead, each partition is 5 days
       # old partitions are never deleted, `max_age` is not set
       # partitions will be named `[table_name]_[year]_[month]_[month day number]`.
       PostgresPartitioningConfig(
           model=MyPartitionedModel,
           strategy=PostgresCurrentTimePartitioningStrategy(
               size=PostgresTimePartitionSize(wdyas=5),
               count=12,
           ),
       ),
   ])

   # these are the default arguments
   partioning_plan = manager.plan(
       skip_create=False,
       skip_delete=False,
       using='default'
   )

   # prints a list of partitions to be created/deleted
   partitioning_plan.print()

   # apply the plan
   partitioning_plan.apply(using='default');


Custom strategy
~~~~~~~~~~~~~~~

You can create a custom partitioning strategy by implementing the :class:`psqlextra.partitioning.PostgresPartitioningStrategy` interface.

You can look at :class:`psqlextra.partitioning.PostgresCurrentTimePartitioningStrategy` as an example.


Switching partitioning strategies
*********************************

When switching partitioning strategies, you might encounter the problem that partitions for part of a particular range already exist. In order to combat this, you can use the :class:`psqlextra.partitioning.PostgresTimePartitioningStrategy` and specify the `start_datetime` parameter. As a result, no partitions will be created before the given date/time.
