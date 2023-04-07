.. include:: ./snippets/postgres_doc_links.rst

Welcome
=======

``django-postgres-extra`` aims to make all of PostgreSQL's awesome features available through the Django ORM. We do this by taking care of all the hassle. As opposed to the many small packages that are available to try to bring a single feature to Django with minimal effort, ``django-postgres-extra`` goes the extra mile with well tested implementations, seamless migrations and much more.

By seamless, we mean that any features we add will work truly seamlessly. You should not have to manually modify your migrations to work with fields and objects provided by this package.

Features
--------
Explore the documentation to learn about all features:

* :ref:`Conflict handling <conflict_handling_page>`

    Adds support for PostgreSQL's ``ON CONFLICT`` syntax for inserts. Supports for ``DO UPDATE`` and ``DO NOTHING``. In other words; single statement, atomic, concurrency safe upserts.

* :ref:`HStore <hstore_page>`

    Built on top Django's built-in support for `hstore`_ fields. Adds support for indices on keys and unique/required constraints. All of these features integrate well with Django's migrations sytem.

* :ref:`Partial unique index <conditional_unique_index_page>`

   Partial (unique) index that only applies when a certain condition is true.

* :ref:`Case insensitive index <case_insensitive_unique_index_page>`

   Case insensitive index, allows searching a column and ignoring the casing.

* :ref:`Table partitioning <table_partitioning_page>`

    Adds support for PostgreSQL 11.x declarative table partitioning.

* :ref:`Truncating tables <truncate_page>`

   Support for ``TRUNCATE TABLE`` statements (including cascading).

* :ref:`Locking models & tables <locking_page>`

   Support for explicit table-level locks.


* :ref:`Creating/dropping schemas <schemas_page>`

    Support for managing Postgres schemas.


.. toctree::
   :maxdepth: 2
   :caption: Overview

   installation
   managers_models
   hstore
   indexes
   conflict_handling
   deletion
   table_partitioning
   expressions
   annotations
   locking
   schemas
   settings
   api_reference
   major_releases
