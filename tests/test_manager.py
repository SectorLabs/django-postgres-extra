import pytest

from django.db import models

from psqlextra.models import PostgresModel

from .fake_model import get_fake_model


def test_manager_truncate():
    """Tests whether truncating a table works."""

    model = get_fake_model({"name": models.CharField(max_length=255)})

    model.objects.create(name="henk1")
    model.objects.create(name="henk2")

    assert model.objects.count() == 2
    model.objects.truncate()
    assert model.objects.count() == 0


@pytest.mark.django_db(transaction=True)
def test_manager_truncate_cascade():
    """Tests whether truncating a table with cascade works."""

    model_1 = get_fake_model({"name": models.CharField(max_length=255)})

    model_2 = get_fake_model(
        {
            "name": models.CharField(max_length=255),
            "model_1": models.ForeignKey(
                model_1, on_delete=models.CASCADE, null=True
            ),
        }
    )

    obj_1 = model_1.objects.create(name="henk1")
    model_2.objects.create(name="henk1", model_1_id=obj_1.id)

    assert model_1.objects.count() == 1
    assert model_2.objects.count() == 1

    model_1.objects.truncate(cascade=True)

    assert model_1.objects.count() == 0
    assert model_2.objects.count() == 0


def test_manager_truncate_quote_name():
    """Tests whether the truncate statement properly quotes the table name."""

    model = get_fake_model(
        {"name": models.CharField(max_length=255)},
        PostgresModel,
        {
            # without quoting, table names are always
            # lower-case, using a capital case table
            # name requires quoting to work
            "db_table": "MyTable"
        },
    )

    model.objects.create(name="henk1")
    model.objects.create(name="henk2")

    assert model.objects.count() == 2
    model.objects.truncate()
    assert model.objects.count() == 0
