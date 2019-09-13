import uuid

import pytest

from django.db import models

from psqlextra.query import ConflictAction

from ..fake_model import get_fake_model

ROW_COUNT = 10000


@pytest.mark.benchmark()
def test_upsert_bulk_naive(benchmark):
    model = get_fake_model(
        {"field": models.CharField(max_length=255, unique=True)}
    )

    rows = []
    random_values = []
    for i in range(0, ROW_COUNT):
        random_value = str(uuid.uuid4())
        random_values.append(random_value)
        rows.append(model(field=random_value))

    model.objects.bulk_create(rows)

    def _native_upsert(model, random_values):
        """Performs a concurrency safe upsert using the native PostgreSQL
        upsert."""

        rows = [dict(field=random_value) for random_value in random_values]

        for row in rows:
            model.objects.on_conflict(["field"], ConflictAction.UPDATE).insert(
                **row
            )

    benchmark(_native_upsert, model, random_values)


@pytest.mark.benchmark()
def test_upsert_bulk_native(benchmark):
    model = get_fake_model(
        {"field": models.CharField(max_length=255, unique=True)}
    )

    rows = []
    random_values = []
    for i in range(0, ROW_COUNT):
        random_value = str(uuid.uuid4())
        random_values.append(random_value)
        rows.append(model(field=random_value))

    model.objects.bulk_create(rows)

    def _native_upsert(model, random_values):
        """Performs a concurrency safe upsert using the native PostgreSQL
        upsert."""

        rows = [dict(field=random_value) for random_value in random_values]

        model.objects.on_conflict(["field"], ConflictAction.UPDATE).bulk_insert(
            rows
        )

    benchmark(_native_upsert, model, random_values)
