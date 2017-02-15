django-postgres-extra
---------------------

.. image:: https://scrutinizer-ci.com/g/SectorLabs/django-postgres-extra/badges/quality-score.png
    :target: https://scrutinizer-ci.com/g/SectorLabs/django-postgres-extra/

.. image:: https://scrutinizer-ci.com/g/SectorLabs/django-postgres-extra/badges/coverage.png
    :target: https://scrutinizer-ci.com/g/SectorLabs/django-postgres-extra/

``django-postgres-extra`` aims to make all of PostgreSQL's awesome features available through the Django ORM. We do this by taking care of all the hassle. As opposed to the many small packages that are available to try to bring a single feature to Django with minimal effort. ``django-postgres-extra`` goes the extra mile, with well tested implementations, seamless migrations and much more.

With seamless we mean that any features we add will work truly seamlessly. You should not have to manually modify your migrations to work with fields and objects provided by this package.

This package requires Python 3.5 or newer and Django 1.10 or newer.

Focus
-----
Currently, we are working on bringing the following features to Django:

* https://www.postgresql.org/docs/9.1/static/hstore.html
   * UNIQUE constraints
   * All operators
   * GiST and GIN indexes

* https://www.postgresql.org/docs/9.1/static/ltree.html
    * All operators
    * All functions
    * Unique indexes
    * GiST and GIN indexes


Installation
------------
1. Install the package from PyPi:

   .. code-block:: bash

        $ pip install django-postgres-extra

2. Add ``postgres_extra`` and ``django.contrib.postgres`` to your ``INSTALLED_APPS``:

   .. code-block:: bash

        INSTALLED_APPS = [
            ....

            'django.contrib.postgres',
            'postgres_extra'
        ]

3. Set the database engine to ``postgres_extra.db``:

   .. code-block:: python

        DATABASES = {
            'default': {
                ...
                'ENGINE': 'postgres_extra.db'
            }
        }
