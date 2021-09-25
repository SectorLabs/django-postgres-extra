.. _installation:

Installation
============

1. Install the package from PyPi:

    .. code-block:: bash

        $ pip install django-postgres-extra

2. Add ``django.contrib.postgres`` and `psqlextra`` to your ``INSTALLED_APPS``:

    .. code-block:: python

        INSTALLED_APPS = [
            ...
            "django.contrib.postgres",
            "psqlextra",
        ]


3. Set the database engine to ``psqlextra.backend``:

    .. code-block:: python

        DATABASES = {
            "default": {
                ...
                "ENGINE": "psqlextra.backend",
            },
        }

    .. note::

        Already using a custom back-end? Set :ref:`POSTGRES_EXTRA_DB_BACKEND_BASE <POSTGRES_EXTRA_DB_BACKEND_BASE>` to your custom back-end.
