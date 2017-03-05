# django-postgres-extra

django-postgres-extra aims to make all of PostgreSQL's awesome features available through the Django ORM. We do this by taking care of all the hassle. As opposed to the many small packages that are available to try to bring a single feature to Django with minimal effort. django-postgres-extra goes the extra mile, with well tested implementations, seamless migrations and much more.

With seamless we mean that any features we add will work truly seamlessly. You should not have to manually modify your migrations to work with fields and objects provided by this package.

## PostgreSQL features
Explore the [Features](/features/) page for detailed instructions and information on how to use all features.

* [hstore](/features/#hstorefield)
    * [`uniqueness`](/features/#uniqueness)
    * [`required`](/features/#required)

* [upsert](/features/#upsert)
    * [`upsert`](http://localhost:8000/features/#upsert_1)
    * [`upsert_and_get`](http://localhost:8000/features/#upsert_and_get)

* [signals](/features/#signals)

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

4. Make sure all models that inherit from `psqlextra.models.PostgresModel` or use the `psqlextra.manager.PostgresManager`. Without this, most features **do not work**.

## Requirements
In order to use this package, your project must be using:

* Python 3.5, or newer
* PostgreSQL 9.6 or newer
* Django 1.10 or newer

Python 3.5 is required because type hints are used. A feature only available in Python 3.5 and newer. PostgreSQL 9.6 is required to take advantage of the latest features such as `ltree`.
