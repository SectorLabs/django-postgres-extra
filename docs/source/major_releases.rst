Major releases
==============


1.x
---

* First release.


2.x
---

New features
************

* Support for PostgreSQL 11.x declarative table partitioning.
* Support for ``TRUNCATE TABLE``
* Case insensitive index


Other changes
*************

* Uses Django 2.x's mechanism for overriding queries and compilers. ``django-postgres-extra`` is extensible in the same way that Django is extensible now.
* Removes hacks because Django 2.x is more extensible.


Breaking changes
****************

* Removes support for ``psqlextra.signals``. Switch to standard Django signals.
* Inserts with ``ConflictAction.NOTHING`` only returns new rows. Conflicting rows are not returned.
* Drop support for Python 3.5.
* Drop support for Django 1.x.
* Removes ``psqlextra.expressions.Min``, ``psqlextra.expressions.Max``, these are natively supported by Django.


FAQ
***

1. Why was ``psqlextra.signals`` removed?

    In order to make ``psqlextra.signals.update`` work, ``django-postgres-extra`` hooked into Django's :meth:`django:django.db.models.query.QuerySet.update` method to add a ``RETURNING id`` clause to the statement. This slowed down all update queries, even if no signal handler was registered. To fix the performance impact, a breaking change was needed.

    The feature had little to do with PostgreSQL itself. This package focuses on making PostgreSQL specific features available in Django.

    Signals being a rarely used feature that slows down unrelated queries was enough motivation to permanently remove it.


2. Why are inserts with ``ConflictAction.NOTHING`` not returning conflicting rows anymore?

    This is standard PostgresQL behavior. ``django-postgres-extra`` v1.x tried to working around this by doing a void ``ON CONFLICT UPDATE``. This trick only worked when inserting one row.

    The work-around had a significant performance impact and was confusing when performing bulk inserts. In that case, only one row would be returned.

    To avoid further confusion, ``ConflictAction.NOTHING`` now follows standard PostgresQL behavior.


3. Why was support dropped for Python 3.5?

    Python 3.6 added support for dataclasses.


4. Why was support dropped for Django 1.x?

    Mainstream support for the last Django 1.11 was dropped in December 2017. Supporting both Django 1.x and Django 2.x was a major pain point. Dropping support for 1.x simplifies ``django-postgres-extra`` and speeds up the development of new features.
