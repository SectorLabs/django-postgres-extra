## Conditional Unique Index

The `ConditionalUniqueIndex` lets you create partial unique indexes in case you ever need `unique together` constraints
on nullable columns.

Before:

```python
from django.db import models

class Model(models.Model):
    class Meta:
        unique_together = ['a', 'b']

    a = models.ForeignKey('some_model', null=True)
    b = models.ForeignKey('some_other_model')

# Works like a charm!
b = B()
Model.objects.create(a=None, b=b)
Model.objects.create(a=None, b=b)
```

After:

    
```python
from django.db import models
from psqlextra.indexes import ConditionalUniqueIndex

class Model(models.Model):
    class Meta:
        indexes = [
            ConditionalUniqueIndex(fields=['a', 'b'], condition='"a" IS NOT NULL'),
            ConditionalUniqueIndex(fields=['b'], condition='"a" IS NULL')
        ]

    a = models.ForeignKey('some_model', null=True)
    b = models.ForeignKey('some_other_model')

# Integrity Error!
b = B()
Model.objects.create(a=None, b=b)
Model.objects.create(a=None, b=b)
```

## Case Sensitive Unique Index
The `CaseSensitiveUniqueIndex` lets you create an index that ignores the casing for the specified field(s).

This makes the field(s) behave more like a text field in MySQL.

```python
from django.db import models
from psqlextra.indexes import CaseSensitiveUniqueIndex

class Model(models.Model):
    class Meta:
        indexes = [
            CaseSensitiveUniqueIndex(fields=['name']),
        ]

    name = models.CharField(max_length=255)

Model.objects.create(name='henk')
Model.objects.create(name='Henk') # raises IntegrityError
```
