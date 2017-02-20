from django.db import models

from .query import PostgresInsertQuery
from .conflict import ConflictAction


class PostgresManager(models.Manager):
    """Adds support for PostgreSQL specifics."""

    def _insert(self, objs, fields, return_id=False, raw=False,
                using=None, conflict_action: ConflictAction=None):
        """Performs a INSERT query.

        Arguments:
            conflict_action:
                Action to take when a conflict occurs.
        """

        # indicate this query is going to perform write
        self._for_write = True

        # use default database if none specified
        if using is None:
            using = self.db

        # build the query
        query = PostgresInsertQuery(self.model)
        query.insert_values(fields, objs, raw=raw)
        query.on_conflict(conflict_action)

        # execute the query to the database
        return query.get_compiler(using=using).execute_sql(return_id)
