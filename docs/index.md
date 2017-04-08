`django-postgres-extra` aims to make all of PostgreSQL's awesome features available through the Django ORM. We do this by taking care of all the hassle. As opposed to the many small packages that are available to try to bring a single feature to Django with minimal effort. django-postgres-extra goes the extra mile, with well tested implementations, seamless migrations and much more.

With seamless we mean that any features we add will work truly seamlessly. You should not have to manually modify your migrations to work with fields and objects provided by this package.

## Features
Explore the documentation to learn about all features:

* [Manager](/manager)
    * [Upserts](/manager/#upserting)

* [HStore](/hstore)
    * [Unique constraint](/hstore/#unique-constraint)
    * [Not null constraint](/hstore/#not-null-constraint)

* [Signals](/signals)
    * [Create](/signals/#psqlextrasignalscreate)
    * [Update](/signals/#psqlextrasignalsupdate)
    * [Delete](/signals/#psqlextrasignalsdelete)

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
