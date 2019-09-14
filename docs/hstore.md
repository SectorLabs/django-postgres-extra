`psqlextra.fields.HStoreField` is based on Django's [HStoreField](https://docs.djangoproject.com/en/1.10/ref/contrib/postgres/fields/#hstorefield) and therefore supports everything Django does natively, plus more.

## HStore extension
`django-postgres-extra` will automatically enable the `hstore` extension in your postgres database if not enabled yet.

If you are not connecting to your database as a super user, this operation might fail.

### Not a super user
If you do not connect to your database as a super user then you'll have to enable the `hstore` extension yourself. You can stop `django-postgres-extra` from trying to automatically enable the extension by adding the following setting to your settings file:

```python
POSTGRES_EXTRA_AUTO_EXTENSION_SET_UP = False
```

## Constraints
### Unique
The `uniqueness` constraint can be added on one or more `hstore` keys, similar to how a `UNIQUE` constraint can be added to a column. Setting this option causes unique indexes to be created on the specified keys.

You can specify a `list` of strings to specify the keys that must be marked as unique:

```python
from psqlextra.fields import HStoreField
from psqlextra.models import PostgresModel

class MyModel(PostgresModel):
    myfield = HStoreField(uniqueness=['key1']

MyModel.objects.create(myfield={'key1': 'value1'})
MyModel.objects.create(myfield={'key1': 'value1'})
```

The second `create` call will fail with a [IntegrityError](https://docs.djangoproject.com/en/1.10/ref/exceptions/#django.db.IntegrityError) because there's already a row with `key1=value1`.

Uniqueness can also be enforced "together", similar to Django's [unique_together](https://docs.djangoproject.com/en/1.10/ref/models/options/#unique-together) by specifying a tuple of fields rather than a single string:

```python
myfield = HStoreField(uniqueness=[('key1', 'key2'), 'key3])
```

In the example above, `key1` and `key2` must unique **together**, and `key3` must unique on its own. By default, none of the keys are marked as "unique".

### Required
The `required` option can be added to ensure that the specified `hstore` keys are set for every row. This is similar to a `NOT NULL` constraint on a column. You can specify a list of `hstore` keys that are required:

```python
from psqlextra.fields import HStoreField
from psqlextra.models import PostgresModel

class MyModel(PostgresModel):
    myfield = HStoreField(required=['key1'])

mymodel.objects.create(myfield={'key1': none})
MyModel.objects.create(myfield={'key2': 'value1'})
```

Both calls to `create` would fail in the example above since they do not provide a non-null value for `key1`. By default, none of the keys are required.
