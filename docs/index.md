# django-postgres-extra

django-postgres-extra aims to make all of PostgreSQL's awesome features available through the Django ORM. We do this by taking care of all the hassle. As opposed to the many small packages that are available to try to bring a single feature to Django with minimal effort. django-postgres-extra goes the extra mile, with well tested implementations, seamless migrations and much more.

With seamless we mean that any features we add will work truly seamlessly. You should not have to manually modify your migrations to work with fields and objects provided by this package.

## Features
We are currently aiming on having the following features available:

* [hstore](https://www.postgresql.org/docs/9.1/static/hstore.html)
    * UNIQUE constaints
    * NOT-NULL constaints

* [ltree](https://www.postgresql.org/docs/9.1/static/ltree.html)

* [native upsert](https://www.postgresql.org/docs/9.5/static/sql-insert.html#SQL-ON-CONFLICT)

## Installation

1. Install the package from PyPi:

        $ pip install django-postgres-extra

2. Add `postgres_extra` and `django.contrib.postgres` to your `INSTALLED_APPS`:

        INSTALLED_APPS = [
            ....

            'django.contrib.postgres',
            'psqlextra'
        ]

3. Set the database engine to `psqlextra.backend`:

        DATABASES = {
            'default': {
                ...
                'ENGINE': 'psqlextra.backend'
            }
        }

## Requirements
In order to use this package, your project must be using:

* Python 3.5, or newer
* PostgreSQL 9.6 or newer

Python 3.5 is required because type hints are used. A feature only available in Python 3.5 and newer. PostgreSQL 9.6 is required to take advantage of the latest features such as `ltree`.
