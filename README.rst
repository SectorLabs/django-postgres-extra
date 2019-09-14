
.. raw:: html

    <h1 align="center">
      <img width="400" src="https://i.imgur.com/79S6OVM.png" alt="django-postgres-extra">
      <br>
      <br>
    </h1>

====================  ============================
**Tests**             |TestsPassing|_
**License**           |LicenseBadge|_
**PyPi**              |PyPiBadge|_
**Django versions**   2.0, 2.1, 2.2
**Python versions**   3.7, 3.8
====================  ============================

.. |TestsPassing| image:: https://circleci.com/gh/SectorLabs/django-postgres-extra/tree/master.svg?style=svg
.. _TestsPassing: https://circleci.com/gh/SectorLabs/django-postgres-extra/tree/master

.. |LicenseBadge| image:: https://img.shields.io/:license-mit-blue.svg
.. _LicenseBadge: http://doge.mit-license.org


.. |PyPiBadge| image:: https://badge.fury.io/py/django-postgres-extra.svg
.. _PyPiBadge:  https://pypi.python.org/pypi/django-postgres-extra

``django-postgres-extra`` aims to make all of PostgreSQL's awesome features available through the Django ORM. We do this by taking care of all the hassle. As opposed to the many small packages that are available to try to bring a single feature to Django with minimal effort. ``django-postgres-extra`` goes the extra mile, with well tested implementations, seamless migrations and much more.

With seamless we mean that any features we add will work truly seamlessly. You should not have to manually modify your migrations to work with fields and objects provided by this package.

Upgrade
-------
Upgrading from v1? Read the change log with new features and breaking changes.

* `v2 change log <http://django-postgres-extra.readthedocs.io/major-releases#2x>`_

Documentation
-------------

* **ReadTheDocs HTML**

  http://django-postgres-extra.readthedocs.io

* **Plain MarkDown**

  https://github.com/SectorLabs/django-postgres-extra/tree/master/docs

Compatibility
-------------

`django-postgres-extra` is test on Python 3.7 - 3.8 with Django 2.0 - 2.2. It is likely that `django-postgres-extra` works with newer Python and Django versions, but this is not explicitely tested and therefor not guarenteed. If you encounter problems on newer versions, please _do_ open an issue or pull request.

Major features
--------------

* `See the full list of features with documentation <http://django-postgres-extra.readthedocs.io/#features>`_

1. **Native upserts**

   * Single query
   * Concurrency safe
   * With bulk support (single query)

2. **Extended support for HStoreField**

   * Unique constraints
   * Null constraints
   * Select individual keys using ``.values()`` or ``.values_list()``

3. **Support for PostgreSQL 11.x declarative table partitioning**

   * Supports both range and list partitioning

4. **Extra expressions**

   * Select indivual hstore keys
   * ``MIN`` and ``MAX`` for multiple value fields such as hstore and array.

5. **Indexes**

   * Custom indexes with conditions.


Desired/future features
-----------------------

* `Desired enhancements <https://github.com/SectorLabs/django-postgres-extra/issues?q=is%3Aopen+is%3Aissue+label%3Aenhancement>`_


Installation
------------

Following the Installation guide in the documention.

* `Installation Guide <http://django-postgres-extra.readthedocs.io/#installation>`_


Working with the code
---------------------

**Prerequisites**

* PostgreSQL 10 or newer.
* Django 2.0 or newer.
* Python 3.7 or newer.

**Getting started**

1. Clone the repository:

   .. code-block:: bash

        λ git clone https://github.com/SectorLabs/django-postgres-extra.git

2. Create a virtual environment:

   .. code-block:: bash

       λ cd django-postgres-extra
       λ virtualenv env
       λ source env/bin/activate

3. Create a postgres user for use in tests (skip if your default user is a postgres superuser):

   .. code-block:: bash

       λ createuser --superuser psqlextra --pwprompt
       λ export DATABASE_URL=postgres://psqlextra:<password>@localhost/psqlextra

   Hint: if you're using virtualenvwrapper, you might find it beneficial to put
   the ``export`` line in ``$VIRTUAL_ENV/bin/postactivate`` so that it's always
   available when using this virtualenv.

4. Install the development/test dependencies:

   .. code-block:: bash

       λ pip install -r requirements/test.txt
       λ pip install -r requirements/analysis.txt

5. Run the tests:

   .. code-block:: bash

       λ tox

6. Run the benchmarks:

   .. code-block:: bash

       λ py.test -c pytest-benchmark.ini

7. Auto-format code, sort imports and auto-fix linting errors:

   .. code-block:: bash

       λ python setup.py fix
