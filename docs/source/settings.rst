.. _settings:

Settings
========

.. _POSTGRES_EXTRA_DB_BACKEND_BASE:

* ``POSTGRES_EXTRA_DB_BACKEND_BASE``

   ``DATABASES[db_name]['ENGINE']`` must be set to ``"psqlextra.backend"``. If you're already using a custom back-end, set ``POSTGRES_EXTRA_DB_BACKEND_BASE`` to your custom back-end. This will instruct ``django-postgres-extra`` to wrap the back-end you specified.

   A good example of where this might be need is if you are using the PostGIS back-end: ``django.contrib.db.backends.postgis``.

   **Default value**: ``django.db.backends.postgresql``

   .. warning::

      The custom back-end you specify must derive from the standard ``django.db.backends.postgresql``.

.. _POSTGRES_EXTRA_AUTO_EXTENSION_SET_UP:

* ``POSTGRES_EXTRA_AUTO_EXTENSION_SET_UP``

   You can stop ``django-postgres-extra`` from automatically trying to enable the ``hstore`` extension on your database. Enabling extensions using ``CREATE EXTENSION`` requires superuser permissions. Disable this behaviour if you are not connecting to your database server using a superuser.

   **Default value:** ``True``

   .. note::

      If set to ``False``, you must ensure that the ``hstore`` extension is enabled on your database manually. If not enabled, any ``hstore`` related functionality will not work.
