from django.db import connection, models

from psqlextra.backend.schema import SchemaEditor
from psqlextra.partitioning import PartitioningMethod

from .util import define_fake_model


def test_schema_editor_create_partitioned_model():
    model = define_fake_model(
        {
            "name": models.CharField(max_length=255),
            "timestamp": models.DateTimeField(auto_now=True),
            "partitioning_method": PartitioningMethod.RANGE,
            "partitioning_key": ["timestamp"],
        }
    )

    schema_editor = SchemaEditor(connection)
    schema_editor.create_partitioned_model(model)
    schema_editor.create_model_partition(
        model, "pt1", "2019-01-01", "2019-02-01"
    )
