from psqlextra.models import PostgresPartitionedModel
from psqlextra.types import PostgresPartitioningMethod

from .fake_model import define_fake_partitioned_model


def test_partitioned_model_abstract():
    """Tests whether :see:PostgresPartitionedModel is abstract."""

    assert PostgresPartitionedModel._meta.abstract


def test_partitioning_model_options_meta():
    """Tests whether the `_partitioning_meta` attribute is available on the
    class (created by the meta class) and not just creating when the model is
    instantiated."""

    assert PostgresPartitionedModel._partitioning_meta


def test_partitioned_model_default_options():
    """Tests whether the default partitioning options are set as expected on.

    :see:PostgresPartitionedModel.
    """

    model = define_fake_partitioned_model()

    assert model._partitioning_meta.method == PostgresPartitioningMethod.RANGE
    assert model._partitioning_meta.key == []


def test_partitioned_model_method_option():
    """Tests whether the `method` partitioning option is properly copied onto
    the options object."""

    model = define_fake_partitioned_model(
        partitioning_options=dict(method=PostgresPartitioningMethod.LIST)
    )

    assert model._partitioning_meta.method == PostgresPartitioningMethod.LIST


def test_partitioned_model_method_option_none():
    """Tests whether setting the `method` partitioning option results in the
    default being set."""

    model = define_fake_partitioned_model(
        partitioning_options=dict(method=None)
    )

    assert model._partitioning_meta.method == PostgresPartitioningMethod.RANGE


def test_partitioned_model_key_option():
    """Tests whether the `key` partitioning option is properly copied onto the
    options object."""

    model = define_fake_partitioned_model(
        partitioning_options=dict(key=["timestamp"])
    )

    assert model._partitioning_meta.key == ["timestamp"]


def test_partitioned_model_key_option_none():
    """Tests whether setting the `key` partitioning option results in the
    default being set."""

    model = define_fake_partitioned_model(partitioning_options=dict(key=None))

    assert model._partitioning_meta.key == []
