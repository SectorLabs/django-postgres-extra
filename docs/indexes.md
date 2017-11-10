## Conditional Unique Index

The `ConditionalUniqueIndex` lets you create partial unique indexes in case you ever need `unique together` constraints
on nullable columns.

Before:

    from django.db import models

    class Model(models.Model):
        class Meta:
            unique_together = ['a', 'b'']

        a = models.ForeignKey('some_model', null=True)
        b = models.ForeignKey('some_other_model')

    # Works like a charm!
    b = B()
    Model.objects.create(a=None, b=b)
    Model.objects.create(a=None, b=b)

After:

    from django.db import models
    from from psqlextra.indexes import ConditionalUniqueIndex

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
