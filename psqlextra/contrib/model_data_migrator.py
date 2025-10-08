# mypy: disable-error-code="attr-defined"

import json
import os
import time

from abc import abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, Type

from django.db import DEFAULT_DB_ALIAS, connections, models, transaction

from psqlextra.locking import PostgresTableLockMode, postgres_lock_model
from psqlextra.schema import PostgresSchema
from psqlextra.settings import (
    postgres_prepend_local_search_path,
    postgres_set_local,
)

from .transaction import no_transaction


@dataclass
class PostgresModelDataMigratorState:
    id: str
    work_schema: PostgresSchema
    backup_schema: PostgresSchema
    default_schema: PostgresSchema
    storage_settings: Dict[str, Any]


class PostgresModelDataMigrator:
    """Helps altering/moving large amounts of data in a table quickly without
    interruptions.

    In simple terms: This class temporarily drops all indices
    and constraints from a table to speed up writes.

    In complicated terms:

        1. Create copy of the table without indices or constraints
           in a separate schema.

           The clone is made in a separate schema so that there
           are no naming conflicts and there is no need to rename
           anything.

        2. Allow the caller to fill the copy.

            This will be an order of magnitude faster because
            there are no indices to build or constraints to
            statisfy. You are responsible for making sure the
            data is ok and will statisfy the constraints when
            they come back.

        3. Add the indices and constraints to the table.

            This takes time, but it's still a lot faster than
            the indices being built incrementally.

        4. Allow the caller to clean up the copied table.

            With the indices back in place, filtering the copied
            table should be fast. Perfect time to clean up
            some data.

        5. Vacuum+Analyze the table.

            Vacuuming ensures we don't risk transaction ID
            wrap-around and analyzing ensures up-to-date
            statistics.

        6. Start a transaction.

        7. Lock the real table in EXCLUSIVE mode.

            This blocks writes or modifications to the table,
            but does not block readers.

        8. Allow the caller to move some data from the real table
           into the copied one.

            This is the perfect time to copy any data that was
            written to the real table since the migration process
            began. Since the original table is locked, you can
            be sure no more rows are being added or modified.

        9. Move the original table into a backup schema.

            This allows it to be quickly restored manually
            if the migration is broken in any way.

        10. Move the copied table in place of the real one.

        11. Commit the transaction, which releases the lock.

    The process is very similiar to how pg_repack rewrites
    an entire table without long-running locks on the table.

    Attributes:
        model: The model to migrate.
        using: Optional name of the database connection to use.
        operation_timeout: Maximum amount of time a single statement
                           can take.
    """

    model: Type[models.Model]
    using: str = DEFAULT_DB_ALIAS
    operation_timeout: timedelta

    def __init__(self, logger) -> None:
        self.logger = logger
        self.connection = connections[self.using]
        self.schema_editor = self.connection.schema_editor(atomic=False)

    @abstractmethod
    def fill_cloned_table_lockless(
        self, work_schema: PostgresSchema, default_schema: PostgresSchema
    ) -> None:
        """Moment to fill the cloned table with data."""

    @abstractmethod
    def clean_cloned_table(
        self, work_schema: PostgresSchema, default_schema: PostgresSchema
    ) -> None:
        """Moment to clean the filled table after it has indices and validated
        data."""

    @abstractmethod
    def fill_cloned_table_locked(
        self, work_schema: PostgresSchema, default_schema: PostgresSchema
    ) -> None:
        """Moment to do final cleaning while the original table is locked for
        writing."""

    @no_transaction(
        why="The transaction would be too big and some statements cannot be run in a transaction."
    )
    def migrate(self) -> PostgresModelDataMigratorState:
        start_time = time.time()

        with self.atomic():
            with self.connection.cursor() as cursor:
                storage_settings = (
                    self.connection.introspection.get_storage_settings(
                        cursor, self.table_name
                    )
                )

            state = PostgresModelDataMigratorState(
                id=os.urandom(4).hex(),
                work_schema=PostgresSchema.create_random(
                    f"migrate_{self.table_name}", using=self.using
                ),
                backup_schema=PostgresSchema.create_random(
                    f"backup_{self.table_name}", using=self.using
                ),
                default_schema=PostgresSchema.default,
                storage_settings=storage_settings,
            )

        logger = self.logger.bind(id=state.id)
        logger.info(
            f"Starting migration of {self.table_name}",
            data=json.dumps(
                {
                    "work_schema": state.work_schema.name,
                    "backup_schema": state.backup_schema.name,
                    "default_schema": state.default_schema.name,
                    "storage_settings": state.storage_settings,
                }
            ),
        )

        count = self.model.objects.using(self.using).count()
        logger.info(f"Found {count} records in {self.table_name}")

        phases = [
            (self._migrate_phase_1, "cloning and filling table"),
            (self._migrate_phase_2, "adding constraints and indexes"),
            (self._migrate_phase_3, "cleaning up and vacuuming"),
            (self._migrate_phase_4, "swapping"),
        ]

        for index, (phase, description) in enumerate(phases):
            phase_start_time = time.time()
            logger.info(
                f"Starting phase #{index + 1} of migrating {self.table_name}: {description}"
            )
            phase(state)
            logger.info(
                f"Finished phase #{index + 1} of migrating {self.table_name}: {description}",
                task_time=time.time() - phase_start_time,
            )

        state.work_schema.delete(cascade=True, using=self.using)

        logger.info(
            f"Finished migrating {self.table_name}",
            task_time=time.time() - start_time,
        )

        return state

    def _migrate_phase_1(self, state: PostgresModelDataMigratorState) -> None:
        """Clone the table without constraints or indices."""

        with self.atomic():
            self.schema_editor.clone_model_structure_to_schema(
                self.model, schema_name=state.work_schema.name
            )

            # Disable auto-vacuum on the cloned table to prevent
            # it from consuming excessive resources _while_ we're
            # writing to it. We're running this manually before
            # we turn it back on in the last phase.
            with postgres_prepend_local_search_path(
                [state.work_schema.name], using=self.using
            ):
                self.schema_editor.alter_model_storage_setting(
                    self.model, "autovacuum_enabled", "false"
                )

        # Let the derived class fill our cloned table
        self.fill_cloned_table_lockless(state.work_schema, state.default_schema)

    def _migrate_phase_2(self, state: PostgresModelDataMigratorState) -> None:
        """Add indices and constraints to the cloned table."""

        # Add indices and constraints to the temporary table
        # This could be speed up by increasing `maintenance_work_mem`
        # and `max_parallel_workers_per_gather`, but we won't as
        # it'll consume more I/O, potentially disturbing normal traffic.
        with self.atomic():
            self.schema_editor.clone_model_constraints_and_indexes_to_schema(
                self.model, schema_name=state.work_schema.name
            )

        # Validate foreign keys
        #
        # The foreign keys have been added in NOT VALID mode so they
        # only validate new rows. Validate the existing rows.
        #
        # This is a two-step process to avoid a AccessExclusiveLock
        # on the referenced tables.
        with self.atomic():
            self.schema_editor.clone_model_foreign_keys_to_schema(
                self.model, schema_name=state.work_schema.name
            )

    def _migrate_phase_3(self, state: PostgresModelDataMigratorState) -> None:
        """Clean & finalize the cloned table."""

        # Let the derived class do some clean up on the temporary
        # table now that we have indices and constraints.
        with self.atomic():
            self.clean_cloned_table(state.work_schema, state.default_schema)

        # Finalize the copy by vacuuming+analyzing it
        #
        # VACUUM: There should not be much bloat since the table
        #         is new, but the clean up phase might have generated some.
        #
        #         We mostly VACUUM to reset the transaction ID and prevent
        #         transaction ID wraparound.
        #
        # ANALYZE: The table went from 0 to being filled, by running ANALYZE,
        #          we update the statistics, allowing the query planner to
        #          make good decisions.
        with postgres_prepend_local_search_path(
            [state.work_schema.name], using=self.using
        ):
            self.schema_editor.vacuum_model(self.model, analyze=True)

        # Re-enable autovacuum on the cloned table
        with postgres_prepend_local_search_path(
            [state.work_schema.name], using=self.using
        ):
            autovacuum_enabled = state.storage_settings.get(
                "autovacuum_enabled"
            )
            if autovacuum_enabled:
                self.schema_editor.alter_model_storage_setting(
                    self.model, "autovacuum_enabled", autovacuum_enabled
                )
            else:
                self.schema_editor.reset_model_storage_setting(
                    self.model, "autovacuum_enabled"
                )

    def _migrate_phase_4(self, state: PostgresModelDataMigratorState) -> None:
        """Replace the original table with the cloned one."""

        with self.atomic():
            # Lock the original table for writing so that the caller
            # is given a chance to do last-minute moving of data.
            postgres_lock_model(
                self.model, PostgresTableLockMode.EXCLUSIVE, using=self.using
            )

            # Let derived class finalize the temporary table while the
            # original is locked. Not much work should happen here.
            self.fill_cloned_table_locked(
                state.work_schema, state.default_schema
            )

            # Move the original table into the backup schema.
            # Disable autovacuum on it so we don't waste resources
            # keeping it clean.
            self.schema_editor.alter_model_storage_setting(
                self.model, "autovacuum_enabled", "false"
            )
            self.schema_editor.alter_model_schema(
                self.model, state.backup_schema.name
            )

            # Move the cloned table in place of the original
            with postgres_prepend_local_search_path(
                [state.work_schema.name], using=self.using
            ):
                self.schema_editor.alter_model_schema(
                    self.model, state.default_schema.name
                )

    @property
    def model_name(self) -> str:
        return self.model.__name__

    @property
    def table_name(self) -> str:
        return self.model._meta.db_table

    @contextmanager
    def atomic(self):
        """Creates a atomic transaction with run-time parameters tuned for a
        live migration.

        - Statement/idle timeout set to prevent runaway queries
          from continuing long after the migrator was killed.
        - No parallel works to keep I/O under control.
        """

        with transaction.atomic(durable=True, using=self.using):
            with postgres_set_local(
                statement_timeout=f"{self.operation_timeout.total_seconds()}s",
                idle_in_transaction_session_timeout=f"{self.operation_timeout.total_seconds()}s",
                max_parallel_workers_per_gather=0,
                using=self.using,
            ):
                yield
