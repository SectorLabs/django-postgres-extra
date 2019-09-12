import datetime

import django
import pytest

from django.db import connection, models, transaction

from psqlextra.backend.schema import SchemaEditor
from psqlextra.partitioning import PartitioningMethod

from .util import define_fake_model


def test_schema_editor_create_partitioned_model():
    model = define_fake_model(
        {
            "name": models.CharField(max_length=255),
            "timestamp": models.DateTimeField(),
            "partitioning_method": PartitioningMethod.RANGE,
            "partitioning_key": ["timestamp"],
        }
    )

    schema_editor = SchemaEditor(connection)
    schema_editor.create_partitioned_model(model)
    schema_editor.add_partition(model, "pt1", "2019-01-01", "2019-02-01")

    with transaction.atomic():
        # there's no partition for this, should fail
        with pytest.raises(django.db.utils.IntegrityError):
            model.objects.create(
                name="swen", timestamp=datetime.date(2018, 1, 1)
            )

    model.objects.create(name="swen", timestamp=datetime.date(2019, 1, 1))
