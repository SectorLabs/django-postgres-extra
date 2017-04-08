from django.db import models
from django.test import TestCase

from psqlextra.util import postgres_manager

from .fake_model import get_fake_model


class ManagerContextTestCase(TestCase):
    """Tests whether the :see:postgres_manager context
    manager works properly."""

    def test_manager_context(self):
        """Tests whether the :see:postgres_manager context
        manager can be used to get access to :see:PostgresManager
        on a model that does not use it directly or inherits
        from :see:PostgresModel."""

        model = get_fake_model({
            'myfield': models.CharField(max_length=255, unique=True)
        }, models.Model)

        with postgres_manager(model) as manager:
            manager.upsert(
                conflict_target=['myfield'],
                fields=dict(
                    myfield='beer'
                )
            )

            assert manager.first().myfield == 'beer'
