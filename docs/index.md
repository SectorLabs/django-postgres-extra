`django-postgres-extra` aims to make all of PostgreSQL's awesome features available through the Django ORM. We do this by taking care of all the hassle. As opposed to the many small packages that are available to try to bring a single feature to Django with minimal effort, `django-postgres-extra` goes the extra mile with well tested implementations, seamless migrations and much more.

By seamless, we mean that any features we add will work truly seamlessly. You should not have to manually modify your migrations to work with fields and objects provided by this package.

## Features
Explore the documentation to learn about all features:

* [Managers & Models](/managers-models)

    Use the custom manager and model to take advantage of all of the features described below. Most features do not work without using the `PostgresManager` and `PostgresModel`. 

    <br>

* [Conflict Handling](/conflict-handling)

    
    Adds support for PostgreSQL's `ON CONFLICT` syntax for inserts. Supports for `DO UPDATE` and `DO NOTHING`. In other words; single statement, atomic, concurrency safe upserts.

    <br>


* [HStore](/hstore)
    
    Built on top Django's built-in support for HStore field. Adds support for indices on keys and unique/required constraints.

    All of these features integrate well with Django's migrations sytem.

    * [Constraints](/hstore/#constraints)
        * [Unique](/hstore/#unique)
        * [Required](/hstore#required)

    <br>

* [Indexes](/indexes)

    Custom index types supported by PostgreSQL, but not by Django.


    * [ConditionalUniqueIndex](/indexes/#conditional-unique-index)
    * [CaseSensitiveUniqueIndex](/indexes/#case-sensitive-unique-index)

    <br>


* [Table Partitioning](/table-partitioning)

    Adds support for PostgreSQL 11.x declarative table partitioning.

    * [Partitioned tables](/table-partitioning#partitioned_tables)
    * [Adding/removing partitions](/table-partitioning#adding_removing_partitions)

    <br>

* [Deletion](/deletion)

    * [TRUNCATE TABLE](/deletion#truncate)

* [Database engine](/db-engine)

## Installation

1. Install the package from PyPi:

        $ pip install django-postgres-extra

2. Add `psqlextra` and `django.contrib.postgres` to your `INSTALLED_APPS`:

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

4. Make sure all models inherit from `psqlextra.models.PostgresModel` or use the `psqlextra.manager.PostgresManager`. Without this, most features **do not work**.

    * [Managers & Models](/managers_models)

    <br>

5. Read the documentation about the custom database engine to avoid common pitfalls.

    Already using a custom database engine or you do not have super user permission on your database? Read the docs below for work-arounds.

    * [Database engine](/db-engine)


## Requirements
In order to use this package, your project must be using:

* Python 3.7, or newer
* PostgreSQL 10.x or newer
* Django 2.0 or newer

Python 3.7 is required because type hints are used. A feature only available in Python 3.7 and newer. PostgreSQL 10.x is required to take advantage of the latest features such as `ltree`.
