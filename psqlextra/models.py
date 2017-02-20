from django.db import models

from .manager import PostgresManager
from .conflict import ConflictAction


class PostgresModel(models.Model):

    class Meta:
        abstract = True
        base_manager_name = 'objects'

    objects = PostgresManager()

    def __init__(self, *args, **kwargs):
        """Initializes a new instance of :see:PostgresModel."""

        super(PostgresModel, self).__init__(*args, **kwargs)

        self._conflict_action = None

    def save(self, *args, on_conflict: ConflictAction=None):
        """Saves the current instance.

        Arguments:
            on_conflict:
                What action to take when a conflict
                arises with another row.
        """

        self._conflict_action = on_conflict
        return super(PostgresModel, self).save(*args)
        self._conflict_action = None

    def _do_insert(self, manager, using, fields, update_pk, raw):
        """Performs a INSERT."""

        return manager._insert(
            [self], fields=fields, return_id=update_pk,
            using=using, raw=raw, conflict_action=self._conflict_action)
