import argparse

from unittest.mock import MagicMock, create_autospec, patch

import pytest

from django.db import models
from django.test import override_settings
from syrupy.extensions.json import JSONSnapshotExtension

from psqlextra.backend.introspection import (
    PostgresIntrospectedPartitionTable,
    PostgresIntrospectedPartitonedTable,
)
from psqlextra.management.commands.pgpartition import Command
from psqlextra.partitioning import PostgresPartitioningManager
from psqlextra.partitioning.config import PostgresPartitioningConfig
from psqlextra.partitioning.partition import PostgresPartition
from psqlextra.partitioning.strategy import PostgresPartitioningStrategy

from .fake_model import define_fake_partitioned_model


@pytest.fixture
def snapshot(snapshot):
    return snapshot.use_extension(JSONSnapshotExtension)


@pytest.fixture
def fake_strategy():
    strategy = create_autospec(PostgresPartitioningStrategy)

    strategy.createable_partition = create_autospec(PostgresPartition)
    strategy.createable_partition.name = MagicMock(return_value="tobecreated")
    strategy.to_create = MagicMock(return_value=[strategy.createable_partition])

    strategy.deleteable_partition = create_autospec(PostgresPartition)
    strategy.deleteable_partition.name = MagicMock(return_value="tobedeleted")
    strategy.to_delete = MagicMock(return_value=[strategy.deleteable_partition])

    return strategy


@pytest.fixture
def fake_model(fake_strategy):
    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    # consistent model name so snapshot tests work
    model.__name__ = "test"

    # we have to trick the system into thinking the model/table
    # actually exists with one partition (so we can simulate deletions)
    deleteable_partition_name = fake_strategy.deleteable_partition.name()
    mocked_partitioned_table = PostgresIntrospectedPartitonedTable(
        name=model._meta.db_table,
        method=model._partitioning_meta.method,
        key=model._partitioning_meta.key,
        partitions=[
            PostgresIntrospectedPartitionTable(
                name=deleteable_partition_name,
                full_name=f"{model._meta.db_table}_{deleteable_partition_name}",
                comment="psqlextra_auto_partitioned",
            )
        ],
    )

    introspection_package = "psqlextra.backend.introspection"
    introspection_class = f"{introspection_package}.PostgresIntrospection"
    get_partitioned_table_path = f"{introspection_class}.get_partitioned_table"

    with patch(get_partitioned_table_path) as mock:
        mock.return_value = mocked_partitioned_table
        yield model


@pytest.fixture
def fake_partitioning_manager(fake_model, fake_strategy):
    manager = PostgresPartitioningManager(
        [PostgresPartitioningConfig(fake_model, fake_strategy)]
    )

    with override_settings(PSQLEXTRA_PARTITIONING_MANAGER=manager):
        yield manager


@pytest.fixture
def run(capsys):
    def _run(*args):
        parser = argparse.ArgumentParser()

        command = Command()
        command.add_arguments(parser)
        command.handle(**vars(parser.parse_args(args)))

        return capsys.readouterr().out

    return _run


@pytest.mark.parametrize("args", ["-d", "--dry"], ids=["d", "dry"])
def test_management_command_partition_dry_run(
    args, snapshot, run, fake_model, fake_partitioning_manager
):
    """Tests whether the --dry option actually makes it a dry run and does not
    create/delete partitions."""

    config = fake_partitioning_manager.find_config_for_model(fake_model)
    assert run(args) == snapshot()

    config.strategy.createable_partition.create.assert_not_called()
    config.strategy.createable_partition.delete.assert_not_called()
    config.strategy.deleteable_partition.create.assert_not_called()
    config.strategy.deleteable_partition.delete.assert_not_called()


@pytest.mark.parametrize("args", ["-y", "--yes"], ids=["y", "yes"])
def test_management_command_partition_auto_confirm(
    args, snapshot, run, fake_model, fake_partitioning_manager
):
    """Tests whether the --yes option makes it not ask for confirmation before
    creating/deleting partitions."""

    config = fake_partitioning_manager.find_config_for_model(fake_model)
    assert run(args) == snapshot

    config.strategy.createable_partition.create.assert_called_once()
    config.strategy.createable_partition.delete.assert_not_called()
    config.strategy.deleteable_partition.create.assert_not_called()
    config.strategy.deleteable_partition.delete.assert_called_once()


@pytest.mark.parametrize(
    "answer",
    ["y", "Y", "yes", "YES"],
    ids=["y", "capital_y", "yes", "capital_yes"],
)
def test_management_command_partition_confirm_yes(
    answer, monkeypatch, snapshot, run, fake_model, fake_partitioning_manager
):
    """Tests whether the --yes option makes it not ask for confirmation before
    creating/deleting partitions."""

    config = fake_partitioning_manager.find_config_for_model(fake_model)

    monkeypatch.setattr("builtins.input", lambda _: answer)
    assert run() == snapshot

    config.strategy.createable_partition.create.assert_called_once()
    config.strategy.createable_partition.delete.assert_not_called()
    config.strategy.deleteable_partition.create.assert_not_called()
    config.strategy.deleteable_partition.delete.assert_called_once()


@pytest.mark.parametrize(
    "answer",
    ["n", "N", "no", "No", "NO"],
    ids=["n", "capital_n", "no", "title_no", "capital_no"],
)
def test_management_command_partition_confirm_no(
    answer, monkeypatch, snapshot, run, fake_model, fake_partitioning_manager
):
    """Tests whether the --yes option makes it not ask for confirmation before
    creating/deleting partitions."""

    config = fake_partitioning_manager.find_config_for_model(fake_model)

    monkeypatch.setattr("builtins.input", lambda _: answer)
    assert run() == snapshot

    config.strategy.createable_partition.create.assert_not_called()
    config.strategy.createable_partition.delete.assert_not_called()
    config.strategy.deleteable_partition.create.assert_not_called()
    config.strategy.deleteable_partition.delete.assert_not_called()
