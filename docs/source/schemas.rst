.. include:: ./snippets/postgres_doc_links.rst

.. _schemas_page:

Schema
======

The :meth:`~psqlextra.schema.PostgresSchema` class provides basic schema management functionality.

Django does **NOT** support custom schemas. This module does not attempt to solve that problem.

This module merely allows you to create/drop schemas and allow you to execute raw SQL in a schema. It is not attempt at bringing multi-schema support to Django.


Reference an existing schema
----------------------------

.. code-block:: python

   for psqlextra.schema import PostgresSchema

   schema = PostgresSchema("myschema")

   with schema.connection.cursor() as cursor:
       cursor.execute("SELECT * FROM tablethatexistsinmyschema")


Checking if a schema exists
---------------------------

.. code-block:: python

   for psqlextra.schema import PostgresSchema

   schema = PostgresSchema("myschema")
   if PostgresSchema.exists("myschema"):
       print("exists!")
   else:
       print('does not exist!")


Creating a new schema
---------------------

With a custom name
******************

.. code-block:: python

   for psqlextra.schema import PostgresSchema

   # will raise an error if the schema already exists
   schema = PostgresSchema.create("myschema")


Re-create if necessary with a custom name
*****************************************

.. warning::

   If the schema already exists and it is non-empty or something is referencing it, it will **NOT** be dropped. Specify ``cascade=True`` to drop all of the schema's contents and **anything referencing it**.

.. code-block:: python

   for psqlextra.schema import PostgresSchema

   # will drop existing schema named `myschema` if it
   # exists and re-create it
   schema = PostgresSchema.drop_and_create("myschema")

   # will drop the schema and cascade it to its contents
   # and anything referencing the schema
   schema = PostgresSchema.drop_and_create("otherschema", cascade=True)


With a time-based name
**********************

.. warning::

   The time-based suffix is precise up to the second. If two threads or processes both try to create a time-based schema name with the same suffix in the same second, they will have conflicts.

.. code-block:: python

   for psqlextra.schema import PostgresSchema

   # schema name will be "myprefix_<timestamp>"
   schema = PostgresSchema.create_time_based("myprefix")
   print(schema.name)


With a random name
******************

A 8 character suffix is appended. Entropy is dependent on your system. See :meth:`~os.urandom` for more information.

.. code-block:: python

   for psqlextra.schema import PostgresSchema

   # schema name will be "myprefix_<8 random characters>"
   schema = PostgresSchema.create_random("myprefix")
   print(schema.name)


Temporary schema with random name
*********************************

Use the :meth:`~psqlextra.schema.postgres_temporary_schema` context manager to create a schema with a random name. The schema will only exist within the context manager.

By default, the schema is not dropped if an exception occurs in the context manager. This prevents unexpected data loss. Specify ``drop_on_throw=True`` to drop the schema if an exception occurs.

Without an outer transaction, the temporary schema might not be dropped when your program is exits unexpectedly (for example; if it is killed with SIGKILL). Wrap the creation of the schema in a transaction to make sure the schema is cleaned up when an error occurs or your program exits suddenly.

.. warning::

   By default, the drop will fail if the schema is not empty or there is anything referencing the schema.  Specify ``cascade=True`` to drop all of the schema's contents and **anything referencing it**.

.. code-block:: python

   for psqlextra.schema import postgres_temporary_schema

   with postgres_temporary_schema("myprefix") as schema:
       pass

   with postgres_temporary_schema("otherprefix", drop_on_throw=True) as schema:
       raise ValueError("drop it like it's hot")

   with postgres_temporary_schema("greatprefix", cascade=True) as schema:
       with schema.connection.cursor() as cursor:
           cursor.execute(f"CREATE TABLE {schema.name} AS SELECT 'hello'")

   with postgres_temporary_schema("amazingprefix", drop_on_throw=True, cascade=True) as schema:
       with schema.connection.cursor() as cursor:
           cursor.execute(f"CREATE TABLE {schema.name} AS SELECT 'hello'")

       raise ValueError("oops")

Deleting a schema
-----------------

Any schema can be dropped, including ones not created by :class:`~psqlextra.schema.PostgresSchema`.

The ``public`` schema cannot be dropped. This is a Postgres built-in and it is almost always a mistake to drop it. A :class:`~django.core.exceptions.SuspiciousOperation` erorr will be raised if you attempt to drop the ``public`` schema.

.. warning::

   By default, the drop will fail if the schema is not empty or there is anything referencing the schema.  Specify ``cascade=True`` to drop all of the schema's contents and **anything referencing it**.

.. code-block:: python

   for psqlextra.schema import PostgresSchema

   schema = PostgresSchema.drop("myprefix")
   schema = PostgresSchema.drop("myprefix", cascade=True)
