.. include:: ./snippets/postgres_doc_links.rst

.. _views_page:


Views & materialized views
==========================

:class:`~psqlextra.models.PostgresViewModel` and :class:`~psqlextra.models.PostgresMaterializedViewModel` add support for `PostgreSQL Views`_ and `PostgreSQL Materialized Views`_.

.. note::

    You can create indices and constraints on (materialized) views just like you would on normal PostgreSQL tables. This is fully supported.


Known limitations
-----------------

Changing view query
*******************

THere is **NO SUPPORT** whatsoever for changing the backing query of a view after the initial creation.

Such changes are not detected by ``python manage.py pgmakemigrations`` and there are no pre-built operations for modifying them.


Creating a (materialized) view
------------------------------

Views are declared like regular Django models with a special base class and an extra option to specify the query backing the view. Once declared, they behave like regular Django models with the exception that you cannot write to them.

Declaring the model
*******************

.. warning::

    All fields returned by the backing query must be declared as Django fields. Fields that are returned by the query that aren't declared as Django fields become
    part of the view, but will not be visible from Django.

With a queryset
~~~~~~~~~~~~~~~

.. code-block:: python

    from django.db import models

    from psqlextra.models import PostgresViewModel


    class MyView(PostgresViewModel):
        name = models.TextField()
        somefk__name = models.TextField()

        class Meta:
            indexes = [models.Index(fields=["name"])]

        class ViewMeta:
            query = SomeOtherModel.objects.values('id', 'name', 'somefk__name')

    class MyMaterializedView(PostgresMaterializedViewModel):
        name = models.TextField()
        somefk__name = models.TextField()

        class Meta:
            indexes = [models.Index(fields=["name"])]

        class ViewMeta:
            query = SomeOtherModel.objects.values('id', 'name', 'somefk__name')

With raw SQL
~~~~~~~~~~~~

Any raw SQL can be used as the backing query for a view. Specify a tuple to pass the values for placeholders.

.. code-block:: python

    from django.db import models

    from psqlextra.models import PostgresViewModel


    class MyView(PostgresViewModel):
        name = models.TextField()
        somefk__name = models.TextField()

        class Meta:
            indexes = [models.Index(fields=["name"])]

        class ViewMeta:
            query = "SELECT id, somefk.name AS somefk__name FROM mytable INNER JOIN somefk ON somefk.id = mytable.somefk_id"

    class MyMaterializedView(PostgresMaterializedViewModel):
        name = models.TextField()
        somefk__name = models.TextField()

        class Meta:
            indexes = [models.Index(fields=["name"])]

        class ViewMeta:
            query = ("SELECT id, somefk.name AS somefk__name FROM mytable INNER JOIN somefk ON somefk.id = mytable.somefk_id WHERE id > %s", 1)


With a callable
~~~~~~~~~~~~~~~

A callable can be used when your query depends on settings or other variables that aren't available at evaluation time. The callable can return raw SQL, raw SQL with params or a queryset.

.. code-block:: python

    from django.db import models

    from psqlextra.models import PostgresViewModel

    def _generate_query():
        return ("SELECT * FROM sometable WHERE app_name = %s", settings.APP_NAME)

    def _build_query():
        return SomeTable.objects.filter(app_name=settings.APP_NAME)


    class MyView(PostgresViewModel):
        name = models.TextField()
        somefk__name = models.TextField()

        class ViewMeta:
            query = _generate_query

    class MyMaterializedView(PostgresMaterializedViewModel):
        name = models.TextField()
        somefk__name = models.TextField()

        class ViewMeta:
            query = _generate_query


Generating a migration
**********************
Run the following command to automatically generate a migration:

.. code-block:: bash

   python manage.py pgmakemigrations

This will generate a migration that creates the view with the specified query as the base.

.. warning::

    Always use ``python manage.py pgmakemigrations`` for view models.

    The model must be created by the :class:`~psqlextra.backend.migrations.operations.PostgresCreateViewModel` or :class:`~psqlextra.backend.migrations.operations.PostgresCreateMaterializedViewModel` operation.

    Do not use the standard ``python manage.py makemigrations`` command for view models. Django will issue a standard :class:`~django:django.db.migrations.operations.CreateModel` operation. Doing this will not create a view and all subsequent operations will fail.


Refreshing a materialized view
------------------------------

Make sure to read the PostgreSQL documentation on refreshing materialized views for caveats: `PostgreSQL Refresh Materialized Views`_.

.. code-block:: python

    # Takes an AccessExclusive lock and blocks till table is re-filled
    MyViewModel.refresh()

    # Allows concurrent read, does block till table is re-filled.
    # Warning: Only works if the view was refreshed at least once before.
    MyViewModel.refresh(concurrently=True)


Creating a materialized view without data
-----------------------------------------

.. warning::

    You cannot query your materialized view until it has been refreshed at least once. After creating the materialized view without data, you must execute a refresh at some point. The first refresh cannot be ``CONCURRENTLY`` (PostgreSQL restriction).

By default, the migration creates the materialized view and executes the first refresh. If you want to avoid this, pass the ``with_data=False`` flag in the :class:`~psqlextra.backend.migrations.operations.PostgresCreateMaterializedViewModel` operation in your generated migration.

.. code-block:: python

   from django.db import migrations, models

   from psqlextra.backend.migrations.operations import PostgresCreateMaterializedViewModel

   class Migration(migrations.Migration):
       operations = [
           PostgresCreateMaterializedViewModel(
                name="myview",
                fields=[...],
                options={...},
                view_options={
                    "query": ...
                },
                # Not the default, creates materialized with `WITH NO DATA`
                with_data=False,
           )
       ]
