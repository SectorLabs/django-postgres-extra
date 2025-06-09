.. include:: ./snippets/postgres_doc_links.rst

Welcome
=======

``django-postgres-extra`` aims to make all of PostgreSQL's awesome features available through the Django ORM. We do this by taking care of all the hassle. As opposed to the many small packages that are available to try to bring a single feature to Django with minimal effort, ``django-postgres-extra`` goes the extra mile with well tested implementations, seamless migrations and much more.

By seamless, we mean that any features we add will work truly seamlessly. You should not have to manually modify your migrations to work with fields and objects provided by this package.

Features
--------
Explore the documentation to learn about all features:

* :ref:`Conflict handling <conflict_handling_page>`

    Adds support for PostgreSQL's ``ON CONFLICT`` syntax for inserts. Supports for ``DO UPDATE`` and ``DO NOTHING``. Single statement, atomic, concurrency safe upserts. Supports conditional updates as well.

* :ref:`Table partitioning <table_partitioning_page>`

    Adds support for PostgreSQL 11.x declarative table partitioning. Fully integrated into Django migrations. Supports all types of partitioning. Includes a command to automatically create time-based partitions.

* :ref:`Views & materialized views <views_page>`

    Adds support for creating views & materialized views as any other model. Fully integrated into Django migrations.

* :ref:`Locking models & tables <locking_page>`

   Support for explicit table-level locks.

* :ref:`Creating/dropping schemas <schemas_page>`

    Support for managing Postgres schemas.

* :ref:`Truncating tables <truncate_page>`

   Support for ``TRUNCATE TABLE`` statements (including cascading).

For Django 3.1 and older:

* :ref:`Partial unique index <conditional_unique_index_page>`

   Partial (unique) index that only applies when a certain condition is true.

* :ref:`Case insensitive index <case_insensitive_unique_index_page>`

   Case insensitive index, allows searching a column and ignoring the casing.

For Django 2.2 and older:

* :ref:`Unique index <unique_index_page>`

   Unique indices that can span more than one field.

* :ref:`HStore key unique & required constraint <hstore_page>`

   Add unique and required constraints in specific hstore keys.


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
   views
   expressions
   annotations
   locking
   schemas
   settings
   api_reference
   major_releases
