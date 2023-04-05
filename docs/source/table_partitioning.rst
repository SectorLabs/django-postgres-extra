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
* ``PARTITION BY HASH``

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
* Use :attr:`psqlextra.types.PostgresPartitioningMethod.HASH` to ``PARTITION BY HASH``

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


Automatically managing partitions
---------------------------------

The ``python manage.py pgpartition`` command can help you automatically create new partitions ahead of time and delete old ones for time-based partitioning.

You can run this command manually as needed, schedule to run it periodically or run it every time you release a new version of your app.

.. warning::

   We DO NOT recommend that you set up this command to automatically delete partitions without manual review.

   Specify ``--skip-delete`` to not delete partitions automatically. Run the command manually periodically without the ``--yes`` flag to review partitions to be deleted.


Command-line options
********************

 ==================== ============= ================ ==================================================================================================== === === === === === ===
  Long flag            Short flag    Default          Description
 ==================== ============= ================ ==================================================================================================== === === === === === ===
  ``--yes``            ``-y``        ``False``        Specifies yes to all questions. You will NOT be asked for confirmation before partition deletion.
  ``--using``          ``-u``        ``'default'``    Optional name of the database connection to use.
  ``--skip-create``                  ``False``        Whether to skip creating partitions.
  ``--skip-delete``                  ``False``        Whether to skip deleting partitions.

 ==================== ============= ================ ==================================================================================================== === === === === === ===


Configuration
*************

In order to use the command, you have to declare an instance of :class:`psqlextra.partitioning.PostgresPartitioningManager` and set ``PSQLEXTRA_PARTITIONING_MANAGER`` to a string with the import path to your instance of :class:`psqlextra.partitioning.PostgresPartitioningManager`.

For example:

.. code-block:: python

   # myapp/partitioning.py
   from psqlextra.partitioning import PostgresPartitioningManager

   manager = PostgresPartitioningManager(...)

   # myapp/settings.py
   PSQLEXTRA_PARTITIONING_MANAGER = 'myapp.partitioning.manager'


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
   from psqlextra.partitioning.config import PostgresPartitioningConfig

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
               size=PostgresTimePartitionSize(days=5),
               count=12,
           ),
       ),
   ])


Changing a time partitioning strategy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When switching partitioning strategies, you might encounter the problem that partitions for part of a particular range already exist.

In order to combat this, you can use the :class:`psqlextra.partitioning.PostgresTimePartitioningStrategy` and specify the `start_datetime` parameter. As a result, no partitions will be created before the given date/time.


Custom strategy
~~~~~~~~~~~~~~~

You can create a custom partitioning strategy by implementing the :class:`psqlextra.partitioning.PostgresPartitioningStrategy` interface.

You can look at :class:`psqlextra.partitioning.PostgresCurrentTimePartitioningStrategy` as an example.


Manually managing partitions
----------------------------

If you are using list or hash partitioning, you most likely have a fixed amount of partitions that can be created up front using migrations or using the schema editor.

Using migration operations
**************************

Adding a range partition
~~~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`~psqlextra.backend.migrations.operations.PostgresAddRangePartition` operation to add a new range partition. Only use this operation when your partitioned model uses :attr:`psqlextra.types.PostgresPartitioningMethod.RANGE`.

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

Use the :class:`~psqlextra.backend.migrations.operations.PostgresAddListPartition` operation to add a new list partition. Only use this operation when your partitioned model uses :attr:`psqlextra.types.PostgresPartitioningMethod.LIST`.

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


Adding a hash partition
~~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`~psqlextra.backend.migrations.operations.PostgresAddHashPartition` operation to add a new list partition. Only use this operation when your partitioned model uses :attr:`psqlextra.types.PostgresPartitioningMethod.HASH`.

.. code-block:: python

   from django.db import migrations, models

   from psqlextra.backend.migrations.operations import PostgresAddHashPartition

   class Migration(migrations.Migration):
       operations = [
           PostgresAddHashPartition(
              model_name="mypartitionedmodel",
              name="pt1",
              modulus=3,
              remainder=1,
           ),
       ]


Adding a default partition
~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`~psqlextra.backend.migrations.operations.PostgresAddDefaultPartition` operation to add a new list partition.

Note that you can only have one default partition per partitioned table/model. An error will be thrown if you try to create a second default partition.

If you used ``python manage.py pgmakemigrations`` to generate a migration for your newly created partitioned model, you do not need this operation. This operation is added automatically when you create a new partitioned model.

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


.. warning::

   Deleting the default partition and leaving your model without a default partition can be dangerous. Rows that do not fit in any other partition will fail to be inserted.


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

Use the :class:`psqlextra.backend.migrations.operations.PostgresDeleteRangePartition` operation to delete an existing range partition. Only use this operation when your partitioned model uses :attr:`psqlextra.types.PostgresPartitioningMethod.RANGE`.

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

Use the :class:`psqlextra.backend.migrations.operations.PostgresDeleteListPartition` operation to delete an existing range partition. Only use this operation when your partitioned model uses :attr:`psqlextra.types.PostgresPartitioningMethod.LIST`.

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


Deleting a hash partition
~~~~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`psqlextra.backend.migrations.operations.PostgresDeleteHashPartition` operation to delete an existing range partition. Only use this operation when your partitioned model uses :attr:`psqlextra.types.PostgresPartitioningMethod.HASH`.

.. code-block:: python

   from django.db import migrations, models

   from psqlextra.backend.migrations.operations import PostgresDeleteHashPartition

   class Migration(migrations.Migration):
       operations = [
           PostgresDeleteHashPartition(
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


Adding a hash partition
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from django.db import connection

   connection.schema_editor().add_hash_partition(
       model=MyPartitionedModel,
       name="pt1",
       modulus=3,
       remainder=1,
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
