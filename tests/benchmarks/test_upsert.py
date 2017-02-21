import uuid

from django.db import models, transaction
from django.db.utils import IntegrityError
import pytest

from ..fake_model import get_fake_model


def _traditional_upsert(model, random_value):
    """Performs a concurrency safe upsert
    the traditional way."""

    try:

        with transaction.atomic():
            return model.objects.create(field=random_value)
    except IntegrityError:
        return model.objects.filter(field=random_value).first()


def _native_upsert(model, random_value):
    """Performs a concurrency safe upsert
    using the native PostgreSQL upsert."""

    return model.objects.upsert_and_get(field=random_value)


@pytest.mark.django_db()
@pytest.mark.benchmark()
def test_traditional_upsert(benchmark):
    model = get_fake_model({
        'field': models.CharField(max_length=255, unique=True)
    })

    random_value = str(uuid.uuid4())[:8]
    model.objects.create(field=random_value)

    benchmark(_traditional_upsert, model, random_value)


@pytest.mark.django_db()
@pytest.mark.benchmark()
def test_native_upsert(benchmark):
    model = get_fake_model({
        'field': models.CharField(max_length=255, unique=True)
    })

    random_value = str(uuid.uuid4())[:8]
    model.objects.create(field=random_value)

    benchmark(_native_upsert, model, random_value)
