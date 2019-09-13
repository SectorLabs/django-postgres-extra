# Table Partitioning

`django-postgres-extra` adds support for [PostgreSQL's 11.x declarative table partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html#DDL-PARTITIONING-DECLARATIVE).

`django-postgres-extra` supports all available partitioning methods:

* Range
* List

## Partitioned tables
Partitioned tables are declared like regular Django models with a special base class and two extra options to set the partitioning method and key.

### Declaring the model
Inherit your model from `psqlextra.models.PostgresPartitionedModel` and set the `partitioning_method` and `partitioning_key` attributes on your model.


`partitioning_key` must be a list of field names to partition by.


```python
from django.db import models

from psqlextra.types import PostgresPartitioningMethod
from psqlextra.models import PostgresPartitionedModel

class MyModel(PostgresPartitionedModel):
    partitioning_method = PostgresPartitioningMethod.RANGE
    partitioning_key = ["timestamp"]

    name = models.TextField()
    timestamp = models.DateTimeField() 
```

### Creating a migration
Use Django's standard `python manage.py makemigrations` to generate a migration to create your model. Once generated, modify the migration file and replace the `django.db.migrations.operations.CreateModel` with `psqlextra.migrations.operations.CreatePartitionedModel`.

## Adding/removing partitions
Postgres does not have support for automatically creating new partitions as needed. Therefor, one must manually add new partitions. Depending on the partitioning method you have chosen, the partition has to be created differently.

Partitions are tables. Each partition must be given a unique name. `django-postgres-extra` does not require you to create a model for each partition because you are not supposed to query partitions directly.

`django-postgres-extra` allows partitions to be managed in various ways. The most common way is to manage partitions from migrations.

### Using migrations
#### Adding a range partition
Use the `psqlextra.migrations.operations.AddRangePartition` operation to add a new range partition. Only use this operation when your partitioned model uses the `PostgresPartitioningMethod.RANGE`.

```python
from django.db import migrations, models

from psqlextra.migrations.operations import AddRangePartition

class Migration(migrations.Migration):
    operations = [
        AddRangePartition(
           model_name="mypartitionedmodel",
           name="pt1",
           from_values="2019-01-01",
           to_values="2019-02-01",
        ),
    ]
```

#### Adding a list partition
Use the `psqlextra.migrations.operations.AddListPartition` operation to add a new list partition. Only use this operation when your partitioned model uses the `PostgresPartitioningMethod.LIST`.

```python
from django.db import migrations, models

from psqlextra.migrations.operations import AddRangePartition

class Migration(migrations.Migration):
    operations = [
        AddListPartition(
           model_name="mypartitionedmodel",
           name="pt1",
           values=["car", "boat"],
        ),
    ]
```

#### Adding a default partition
Use the `psqlextra.migrations.operations.AddDefaultPartition` operation to add a new default partition. A default partition is the partition where records get saved that couldn't fit in any other partition.

Note that you can only have one default partition per partitioned table/model.

```python
from django.db import migrations, models

from psqlextra.migrations.operations import AddDefaultPartition

class Migration(migrations.Migration):
    operations = [
        AddDefaultPartition(
           model_name="mypartitionedmodel",
           name="default",
        ),
    ]
```

#### Deleting a partition
Use the `psqlextra.migrations.operations.DeletePartition` operation to delete an existing partition.

```python
from django.db import migrations, models

from psqlextra.migrations.operations import DeletePartition

class Migration(migrations.Migration):
    operations = [
        DeletePartition(
           model_name="mypartitionedmodel",
           name="pt1",
        ),
    ]
```

### Using the schema editor
Use the `psqlextra.backend.PostgresSchemaEditor` to manage partitions directly in a more imperative fashion. The schema editor is used by the migration operations described above.

#### Adding a range partition
```python
from django.db import connection

connection.schema_editor.add_range_partition(
    model=MyPartitionedModel,
    name="pt1",
    from_values="2019-01-01",
    to_values="2019-02-01",
)
```

#### Adding a list partition
```python
from django.db import connection

connection.schema_editor.add_list_partition(
    model=MyPartitionedModel,
    name="pt1",
    values=["car", "boat"],
)
```

#### Adding a default partition
```python
from django.db import connection

connection.schema_editor.add_default_partition(
    model=MyPartitionedModel,
    name="default",
)
```

#### Deleting a partition
```python
from django.db import connection

connection.schema_editor.delete_partition(
    model=MyPartitionedModel,
    name="default",
)
```
