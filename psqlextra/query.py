from django.db import models
from django.db.models.sql.subqueries import InsertQuery

from .conflict import ConflictAction


class PostgresInsertQuery(InsertQuery):
    """PostgreSQL specific INSERT query.

    Adds support for ON CONFLICT."""

    def __init__(self, *args, **kwargs):
        super(PostgresInsertQuery, self).__init__(*args, **kwargs)

        self.conflict_action = None

    def on_conflict(self, conflict_action: ConflictAction):
        """Sets what this does query should do when a
        conflict with another row arises.

        Arguments:
            conflict_action:
                The action to take when a conflict
                with another row arises.
        """

        self.conflict_action = conflict_action

