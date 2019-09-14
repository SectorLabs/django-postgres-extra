`psqlextra.backend` is an extension of the standard PostgreSQL back-end that comes with Django. This is required in order to implement automatic migrations for all the extra features that `psqlextra` offers.

## Superuser required
The `psqlextra.backend` attempts to enable the `hstore` extension through the following statement:

```sql
CREATE EXTENSION IF NOT EXISTS 'hstore'
```

This requires the user connecting to the database to be a super-user. If you're using a non-superuser to connect to your database, make sure to manually enable the extension on your database, otherwise migrations will fail with an exception:

```
django.db.utils.ProgrammingError: permission denied to create extension `hstore`
HINT:  Must be superuser to create this extension.
```

## Disable auto extension set up
You can stop `django-postgres-extra` from trying to enable the `hstore` extension by setting this setting:

```python
POSTGRES_EXTRA_AUTO_EXTENSION_SET_UP = False
```

## Using a custom database back-end
Are you already using a custom database back-end that is not the standard PostgreSQL back-end? For example, the `postgis` back-end? You can control the base back-end for `psqlextra` by setting this setting:

```python
POSTGRES_EXTRA_DB_BACKEND_BASE = 'django.contrib.gis.db.backends.postgis'
```

`psqlextra` will use the specified back-end as the base. By default, this is the standard PostgreSQL back-end that comes with Django.
