<h1 align="center">
  <img width="400" src="https://i.imgur.com/79S6OVM.png" alt="django-postgres-extra">
</h1>
  
|  |  |  |
|--------------------|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| :white_check_mark: | **Tests** | [![CircleCI](https://circleci.com/gh/SectorLabs/django-postgres-extra/tree/master.svg?style=svg)](https://circleci.com/gh/SectorLabs/django-postgres-extra/tree/master) |
| :memo: | **License** | [![License](https://img.shields.io/:license-mit-blue.svg)](http://doge.mit-license.org) |
| :package: | **PyPi** | [![PyPi](https://badge.fury.io/py/django-postgres-extra.svg)](https://pypi.python.org/pypi/django-postgres-extra) |
| :four_leaf_clover: | **Code coverage** | [![Coverage Status](https://coveralls.io/repos/github/SectorLabs/django-postgres-extra/badge.svg?branch=coveralls)](https://coveralls.io/github/SectorLabs/django-postgres-extra?branch=master) |
| <img src="https://cdn.iconscout.com/icon/free/png-256/django-1-282754.png" width="22px" height="22px" align="center" /> | **Django Versions** | 2.0, 2.1, 2.2, 3.0, 3.1, 3.2, 4.0 |
| <img src="http://www.iconarchive.com/download/i73027/cornmanthe3rd/plex/Other-python.ico" width="22px" height="22px" align="center" /> | **Python Versions** | 3.6, 3.7, 3.8, 3.9, 3.10 |
| :book: | **Documentation** | [Read The Docs](https://django-postgres-extra.readthedocs.io/en/master/) |
| :warning: | **Upgrade** | [Upgrade from v1.x](https://django-postgres-extra.readthedocs.io/en/master/major_releases.html#new-features)
| :checkered_flag: | **Installation** | [Installation Guide](https://django-postgres-extra.readthedocs.io/en/master/installation.html) |
| :fire: | **Features** | [Features & Documentation](https://django-postgres-extra.readthedocs.io/en/master/index.html#features) |
| :droplet: | **Future enhancements** | [Potential features](https://github.com/SectorLabs/django-postgres-extra/issues?q=is%3Aopen+is%3Aissue+label%3Aenhancement) |

`django-postgres-extra` aims to make all of PostgreSQL's awesome features available through the Django ORM. We do this by taking care of all the hassle. As opposed to the many small packages that are available to try to bring a single feature to Django with minimal effort. ``django-postgres-extra`` goes the extra mile, with well tested implementations, seamless migrations and much more.
 
With seamless we mean that any features we add will work truly seamlessly. You should not have to manually modify your migrations to work with fields and objects provided by this package.

---

:warning: **This README is for v2. See the `v1` branch for v1.x.**

---

## Major features

[See the full list](http://django-postgres-extra.readthedocs.io/#features)

* **Native upserts**

    * Single query
    * Concurrency safe
    * With bulk support (single query)

* **Extended support for HStoreField**

    * Unique constraints
    * Null constraints
    * Select individual keys using ``.values()`` or ``.values_list()``

* **PostgreSQL 11.x declarative table partitioning**

    * Supports both range and list partitioning

* **Faster deletes**

    * Truncate tables (with cascade)

* **Indexes**

    * Conditional unique index.
    * Case sensitive unique index.

## Working with the code
### Prerequisites

* PostgreSQL 10 or newer.
* Django 2.0 or newer (including 3.x, 4.x).
* Python 3.6 or newer.

### Getting started

1. Clone the repository:

        λ git clone https://github.com/SectorLabs/django-postgres-extra.git

2. Create a virtual environment:

       λ cd django-postgres-extra
       λ virtualenv env
       λ source env/bin/activate

3. Create a postgres user for use in tests (skip if your default user is a postgres superuser):

       λ createuser --superuser psqlextra --pwprompt
       λ export DATABASE_URL=postgres://psqlextra:<password>@localhost/psqlextra

   Hint: if you're using virtualenvwrapper, you might find it beneficial to put
   the ``export`` line in ``$VIRTUAL_ENV/bin/postactivate`` so that it's always
   available when using this virtualenv.

4. Install the development/test dependencies:

       λ pip install .[test] .[analysis]

5. Run the tests:

       λ tox

6. Run the benchmarks:

       λ py.test -c pytest-benchmark.ini

7. Auto-format code, sort imports and auto-fix linting errors:

       λ python setup.py fix
