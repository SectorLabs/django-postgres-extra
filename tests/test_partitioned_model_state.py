import uuid

import pytest

from django.apps import apps
from django.db import models

from psqlextra.backend.migrations.state import (
    PostgresPartitionedModelState,
    PostgresPartitionState,
)
from psqlextra.manager import PostgresManager
from psqlextra.models import PostgresPartitionedModel
from psqlextra.types import PostgresPartitioningMethod

from .fake_model import define_fake_partitioned_model


@pytest.fixture
def model():
    fields = {"name": models.TextField(), "category": models.TextField()}

    partitioning_options = {
        "method": PostgresPartitioningMethod.LIST,
        "key": ["category"],
    }

    model = define_fake_partitioned_model(fields, partitioning_options)
    return model


def test_partitioned_model_state_copies():
    """Tests whether cloning the model state properly copies all the options.

    If it does not copy them, bad things can happen as the state is
    mutated to build up migration state.
    """

    options = dict(method=PostgresPartitioningMethod.RANGE, key=["timestamp"])

    state = PostgresPartitionedModelState(
        app_label="tests",
        name=str(uuid.uuid4()),
        fields=[],
        options=None,
        partitioning_options=options,
        bases=(PostgresPartitionedModel,),
    )

    assert options is not state.partitioning_options


def test_partitioned_model_state_from_model(model):
    """Tests whether creating state from an existing model works as
    expected."""

    state = PostgresPartitionedModelState.from_model(model)
    assert state.partitions == {}
    assert (
        state.partitioning_options["method"] == model._partitioning_meta.method
    )
    assert state.partitioning_options["key"] == model._partitioning_meta.key


def test_partitioned_model_clone(model):
    """Tests whether cloning the state actually clones the partitioning
    options.

    If its not a copy, but a reference instead, bad things can happen as
    the options are mutated to build up migration state.
    """

    state = PostgresPartitionedModelState.from_model(model)
    state.partitions = {
        "pt1": PostgresPartitionState(
            app_label="tests", model_name="tests", name="pt1"
        )
    }

    state_copy = state.clone()
    assert state.partitions is not state_copy.partitions
    assert state.partitioning_options is not state_copy.partitioning_options


def test_partitioned_model_render(model):
    """Tests whether the state can be rendered into a valid model class."""

    options = dict(method=PostgresPartitioningMethod.RANGE, key=["timestamp"])

    state = PostgresPartitionedModelState(
        app_label="tests",
        name=str(uuid.uuid4()),
        fields=[("name", models.TextField())],
        options=None,
        partitioning_options=options,
        bases=(PostgresPartitionedModel,),
        managers=[("cookie", PostgresManager())],
    )

    rendered_model = state.render(apps)

    assert issubclass(rendered_model, PostgresPartitionedModel)
    assert rendered_model.name
    assert isinstance(rendered_model.objects, PostgresManager)
    assert isinstance(rendered_model.cookie, PostgresManager)
    assert rendered_model.__name__ == state.name
    assert rendered_model._meta.apps == apps
    assert rendered_model._meta.app_label == "tests"
    assert rendered_model._partitioning_meta.method == options["method"]
    assert rendered_model._partitioning_meta.key == options["key"]
