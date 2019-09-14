## 1.x
* First release.

## 2.x
### New features
* Support for PostgreSQL 11.x declarative table partitioning.

### Other changes
* Uses Django 2.x's mechanism for overriding queries and compilers. `django-postgres-extra` is extensible in the same way that Django is extensible now.
* Removes hacks because Django 2.x is more extensible.

### Breaking changes
* Removes support for `psqlextra.signals`. Switch to standard Django signals.
* Inserts with `ConflictAction.NOTHING` only returns new rows. Conflicting rows are not returned.
* Drop support for Python 3.5 and 3.6.
* Drop support for Django 1.x.
