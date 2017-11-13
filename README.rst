Django Postgres Extra
=====================

=================  ===================
**Quality**           |QualityBadge|_
**Test coverage**     |CoverageBadge|_
**License**           |LicenseBadge|_
**PyPi**              |PyPiBadge|_
=================  =================== 

.. |QualityBadge| image:: https://scrutinizer-ci.com/g/SectorLabs/django-postgres-extra/badges/quality-score.png
.. _QualityBadge: https://scrutinizer-ci.com/g/SectorLabs/django-postgres-extra/


.. |CoverageBadge| image:: https://scrutinizer-ci.com/g/SectorLabs/django-postgres-extra/badges/coverage.png
.. _CoverageBadge: https://scrutinizer-ci.com/g/SectorLabs/django-postgres-extra/


.. |LicenseBadge| image:: https://img.shields.io/:license-mit-blue.svg
.. _LicenseBadge: http://doge.mit-license.org


.. |PyPiBadge| image:: https://badge.fury.io/py/django-postgres-extra.svg
.. _PyPiBadge:  https://pypi.python.org/pypi/django-postgres-extra

``django-postgres-extra`` aims to make all of PostgreSQL's awesome features available through the Django ORM. We do this by taking care of all the hassle. As opposed to the many small packages that are available to try to bring a single feature to Django with minimal effort. ``django-postgres-extra`` goes the extra mile, with well tested implementations, seamless migrations and much more.

With seamless we mean that any features we add will work truly seamlessly. You should not have to manually modify your migrations to work with fields and objects provided by this package.

Documentation
-------------

* **ReadTheDocs HTML**
   
  http://django-postgres-extra.readthedocs.io

* **Plain MarkDown**

  https://github.com/SectorLabs/django-postgres-extra/tree/master/docs

Major features
--------------

1. **Native upserts**

   * Single query
   * Concurrency safe
   * With bulk support (single query)

2. **Extended support for ``HStoreField``**

   * Unique constraints
   * Null constraints
   * Select individual keys using ``.values()`` or ``.values_list()``

3. **Extra signals**

   * Updates

4. **Extra expressions**

   * ``MIN`` and ``MAX`` for multiple value fields such as hstore and array.

5. **Indexes**

   * Custom indexes with conditions.

Desired/future features
-----------------------

* `Desired enhancements <https://github.com/SectorLabs/django-postgres-extra/issues?q=is%3Aopen+is%3Aissue+label%3Aenhancement>`_


Installation
------------

1. Install the package from PyPi:

   .. code-block:: bash

        λ pip install django-postgres-extra

2. Add ``postgres_extra`` and ``django.contrib.postgres`` to your ``INSTALLED_APPS``:

   .. code-block:: python

        INSTALLED_APPS = [
            ....

            'django.contrib.postgres',
            'psqlextra'
        ]

3. Set the database engine to ``psqlextra.backend``:

   .. code-block:: python

        DATABASES = {
            'default': {
                ...
                'ENGINE': 'psqlextra.backend'
            }
        }

4. Make sure all models that inherit from ``psqlextra.models.PostgresModel`` or use the ``psqlextra.manager.PostgresManager``. Without this, most features **do not work**.


FAQ - Frequently asked questions
--------------------------------

1. **Why do I need to change the database back-end/engine?**

   We utilize PostgreSQL's `hstore` data type, which allows you to store key-value pairs in a column.  In order to create `UNIQUE` constraints on specific key, we need to create a special type of index. We could do this without a custom database back-end, but it would require everyone to manually write their migrations. By using a custom database back-end, we added support for this. When changing the `uniqueness` constraint on a `HStoreField`, our custom database back-end takes care of creating, updating and deleting these constraints/indexes in the database.

2. **I am already using a custom database back-end, can I still use yours?**

   Yes. You can set the ``POSTGRES_EXTRA_DB_BACKEND_BASE`` setting to your current back-end. This will instruct our custom database back-end to inherit from the database back-end you specified. **Warning**: this will only work if the base you specified indirectly inherits from the standard PostgreSQL database back-end.

3. **Does this package work with Python 2?**

   No. Only Python 3.5 or newer is supported. We're using type hints. These do not work well under older versions of Python.

4. **Does this package work with Django 1.X?**

   No. Only Django 1.10 or newer is supported.


Working with the code
---------------------

**Prerequisites**

* PostgreSQL 9.6 or newer.
* Django 1.10 or newer.
* Python 3.5 or newer.

**Getting started**

1. Clone the repository:

   .. code-block:: bash
    
        λ git clone https://github.com/SectorLabs/django-postgres-extra.git

2. Create a virtual environment:

   .. code-block:: bash
    
       λ cd django-postgres-extra
       λ virtualenv env
       λ source env/bin/activate

3. Install the development/test dependencies:

   .. code-block:: bash
    
       λ pip install -r requirements/test.txt
    
4. Run the tests:

   .. code-block:: bash
    
       λ py.test
    
5. Run the benchmarks:

   .. code-block:: bash
    
       λ py.test -c pytest-benchmark.ini
