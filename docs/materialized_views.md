`django-postgres-extra` has great support for PostgresSQL's materialized views. It is fully integrated with Django's migration system. This means that when defining materialized views in your code base, you can expect a migration to be automatically generated to create the view in your database. The same goes for updating or deleting the view.

## Defining a view
Defining a view is as easy as defining a new model:

```python
from psqlextra.models import PostgresMaterializedViewModel

class MyView(PostgresMaterializedViewModel):

    class Meta:
        view_query = MyRealModel.objects.values('name')

    name = models.CharField(max_length=255)
```

Create and execute a migration to create the view in your database:

```python
$ python manage.py makemigrations
$ python manage.py migrate
```

You can now query your materialized view like you query any Django model:

```python
MyView.objects.first().name
```

## Refreshing a view
Materialized view need to be manually refreshed. `django-postges-extra` allows you to refresh your materialized view any time you need.

```python
MyView.view.refresh()
MyView.view.refresh(concurrently=False)
```

By default, the view will be refreshed concurrently by PostgreSQL. This means that your Python code won't wait for the view to be fully refreshed. If you really need to wait for the view to be refreshed, then pass `concurrently=True`.
