.. include:: ./snippets/postgres_doc_links.rst

.. _locking_page:

Locking
=======

`Explicit table-level locks`_ are supported through the :meth:`psqlextra.locking.postgres_lock_model` and :meth:`psqlextra.locking.postgres_lock_table` methods. All table-level lock methods are supported.

Locks are always bound to the current transaction and are released when the transaction is committed or rolled back. There is no support (in PostgreSQL) for explicitly releasing a lock.

.. warning::

    Locks are only released when the *outer* transaction commits or when a nested transaction is rolled back. You can ensure that the transaction you created is the outermost one by passing the ``durable=True`` argument to ``transaction.atomic``.

.. note::

    Use `django-pglocks <https://pypi.org/project/django-pglocks/>`_ if you need a advisory lock.

Locking a model
---------------

Use :class:`psqlextra.locking.PostgresTableLockMode` to indicate the type of lock to acquire.

.. code-block:: python

    from django.db import transaction

    from psqlextra.locking import PostgresTableLockMode, postgres_lock_table

    with transaction.atomic(durable=True):
        postgres_lock_model(MyModel, PostgresTableLockMode.EXCLUSIVE)

    # locks are released here, when the transaction committed


Locking a table
---------------

Use :meth:`psqlextra.locking.postgres_lock_table` to lock arbitrary tables in arbitrary schemas.

.. code-block:: python

    from django.db import transaction

    from psqlextra.locking import PostgresTableLockMode, postgres_lock_table

    with transaction.atomic(durable=True):
        postgres_lock_table("mytable", PostgresTableLockMode.EXCLUSIVE)
        postgres_lock_table(
            "tableinotherschema",
            PostgresTableLockMode.EXCLUSIVE,
            schema_name="myschema"
        )

    # locks are released here, when the transaction committed
