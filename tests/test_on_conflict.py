from django.db import models
from django.test import TestCase

from .fake_model import get_fake_model
from psqlextra.query import ConflictAction


class OnConflictTestCase(TestCase):

    def test_on_conflict(self):
        model = get_fake_model({
            'myfield': models.CharField(max_length=255, unique=True)
        })
