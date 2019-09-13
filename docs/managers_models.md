# Managers & Models

`django-postgres-extra` provides the `psqlextra.manager.PostgresManager` which exposes a lot of functionality. Your model must use this manager in order to use most of this package's functionality.

There are four ways to do this:

* **Inherit your model from `psqlextra.models.PostgresModel`:**

```python
from psqlextra.models import PostgresModel

class MyModel(PostgresModel):
    myfield = models.CharField(max_length=255)
```


* **Override default manager with `psqlextra.manager.PostgresManager`:**

```python
from django.db import models
from psqlextra.manager import PostgresManager

class MyModel(models.Model):
    # override default django manager
    objects = PostgresManager()

    myfield = models.CharField(max_length=255)
```


* **Provide `psqlextra.manager.PostgresManager` as a custom manager:**

```python
from django.db import models
from psqlextra.manager import PostgresManager

class MyModel(models.Model):
    # custom mananger name
    beer = PostgresManager()

    myfield = models.CharField(max_length=255)

# use like this:
MyModel.beer.upsert(..)

# not like this:
MyModel.objects.upsert(..) # error!
```

* **Use the `psqlextra.util.postgres_manager` on the fly:**

    This allows the manager to be used **anywhere** on **any** model, but only within the context. This is especially useful if you want to do upserts into Django's `ManyToManyField`'s generated `through` table:

```python
from django.db import models
from psqlextra.util import postgres_manager

class MyModel(models.Model):
    myself = models.ManyToManyField('self')

# within the context, you can access psqlextra features
with postgres_manager(MyModel.myself.through) as manager:
    manager.upsert(...)
```
