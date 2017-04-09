import uuid

from django.test import TestCase
from django.db import models, transaction
from django.db.utils import IntegrityError
import pytest

from ..fake_model import get_fake_model


@pytest.mark.django_db()
@pytest.mark.benchmark()
class TestUpsert(TestCase):

    @pytest.mark.benchmark()
    @staticmethod
    def test_upsert_traditional(benchmark):
        model = get_fake_model({
            'field': models.CharField(max_length=255, unique=True)
        })

        random_value = str(uuid.uuid4())[:8]
        model.objects.create(field=random_value)

        def _traditional_upsert(model, random_value):
            """Performs a concurrency safe upsert
            the traditional way."""

            try:

                with transaction.atomic():
                    return model.objects.create(field=random_value)
            except IntegrityError:
                model.objects.update(field=random_value)
                return model.objects.get(field=random_value)

        benchmark(_traditional_upsert, model, random_value)

    @pytest.mark.benchmark()
    @staticmethod
    def test_upsert_native(benchmark):
        model = get_fake_model({
            'field': models.CharField(max_length=255, unique=True)
        })

        random_value = str(uuid.uuid4())[:8]
        model.objects.create(field=random_value)

        def _native_upsert(model, random_value):
            """Performs a concurrency safe upsert
            using the native PostgreSQL upsert."""

            return model.objects.upsert_and_get(
                conflict_target=['field'],
                fields=dict(field=random_value)
            )

        benchmark(_native_upsert, model, random_value)
