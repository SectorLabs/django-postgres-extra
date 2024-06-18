import pytest

from django.db import models

from psqlextra.fields import HStoreField
from psqlextra.query import ConflictAction

from .fake_model import get_fake_model


def test_on_conflict_nothing():
    """Tests whether simple insert NOTHING works correctly."""

    model = get_fake_model(
        {
            "title": HStoreField(uniqueness=["key1"]),
            "cookies": models.CharField(max_length=255, null=True),
        }
    )

    # row does not conflict, new row should be created
    obj1 = model.objects.on_conflict(
        [("title", "key1")], ConflictAction.NOTHING
    ).insert_and_get(title={"key1": "beer"}, cookies="cheers")

    obj1.refresh_from_db()
    assert obj1.title["key1"] == "beer"
    assert obj1.cookies == "cheers"

    # row conflicts, no new row should be created
    obj2 = model.objects.on_conflict(
        [("title", "key1")], ConflictAction.NOTHING
    ).insert_and_get(title={"key1": "beer"}, cookies="choco")

    assert not obj2

    # assert that the 'cookies' field didn't change
    obj1.refresh_from_db()
    assert obj1.title["key1"] == "beer"
    assert obj1.cookies == "cheers"
    assert model.objects.count() == 1


def test_on_conflict_nothing_foreign_primary_key():
    """Tests whether simple insert NOTHING works correctly when the primary key
    of a field is a foreign key with a custom name."""

    referenced_model = get_fake_model({})

    model = get_fake_model(
        {
            "parent": models.OneToOneField(
                referenced_model, primary_key=True, on_delete=models.CASCADE
            ),
            "cookies": models.CharField(max_length=255),
        }
    )

    referenced_obj = referenced_model.objects.create()

    # row does not conflict, new row should be created
    obj1 = model.objects.on_conflict(
        ["parent_id"], ConflictAction.NOTHING
    ).insert_and_get(parent_id=referenced_obj.pk, cookies="cheers")

    obj1.refresh_from_db()
    assert obj1.parent == referenced_obj
    assert obj1.cookies == "cheers"

    # row conflicts, no new row should be created
    obj2 = model.objects.on_conflict(
        ["parent_id"], ConflictAction.NOTHING
    ).insert_and_get(parent_id=referenced_obj.pk, cookies="choco")

    assert not obj2

    obj1.refresh_from_db()
    assert obj1.cookies == "cheers"
    assert model.objects.count() == 1


def test_on_conflict_nothing_foreign_key_by_object():
    """Tests whether simple insert NOTHING works correctly when the potentially
    conflicting field is a foreign key specified as an object."""

    other_model = get_fake_model({})

    model = get_fake_model(
        {
            "other": models.OneToOneField(
                other_model, on_delete=models.CASCADE
            ),
            "data": models.CharField(max_length=255),
        }
    )

    other_obj = other_model.objects.create()

    # row does not conflict, new row should be created
    obj1 = model.objects.on_conflict(
        ["other"], ConflictAction.NOTHING
    ).insert_and_get(other=other_obj, data="some data")

    assert obj1.other == other_obj
    assert obj1.data == "some data"

    obj1.refresh_from_db()
    assert obj1.other == other_obj
    assert obj1.data == "some data"

    with pytest.raises(ValueError):
        (
            model.objects.on_conflict(
                ["other"], ConflictAction.NOTHING
            ).insert_and_get(other=obj1)
        )

    # row conflicts, no new row should be created
    obj2 = model.objects.on_conflict(
        ["other"], ConflictAction.NOTHING
    ).insert_and_get(other=other_obj, data="different data")

    assert not obj2

    obj1.refresh_from_db()
    assert model.objects.count() == 1
    assert obj1.other == other_obj
    assert obj1.data == "some data"


def test_on_conflict_nothing_foreign_key_by_id():
    """Tests whether simple insert NOTHING works correctly when the potentially
    conflicting field is a foreign key specified as an id."""

    other_model = get_fake_model({})

    model = get_fake_model(
        {
            "other": models.OneToOneField(
                other_model, on_delete=models.CASCADE
            ),
            "data": models.CharField(max_length=255),
        }
    )

    other_obj = other_model.objects.create()

    # row does not conflict, new row should be created
    obj1 = model.objects.on_conflict(
        ["other_id"], ConflictAction.NOTHING
    ).insert_and_get(other_id=other_obj.pk, data="some data")

    assert obj1.other == other_obj
    assert obj1.data == "some data"

    obj1.refresh_from_db()
    assert obj1.other == other_obj
    assert obj1.data == "some data"

    # row conflicts, no new row should be created
    obj2 = model.objects.on_conflict(
        ["other_id"], ConflictAction.NOTHING
    ).insert_and_get(other_id=other_obj.pk, data="different data")

    assert not obj2
    assert model.objects.count() == 1

    obj1.refresh_from_db()
    assert obj1.other == other_obj
    assert obj1.data == "some data"


@pytest.mark.parametrize(
    "rows,expected_row_count",
    [
        ([dict(amount=1), dict(amount=1)], 1),
        (iter([dict(amount=1), dict(amount=1)]), 1),
        ((row for row in [dict(amount=1), dict(amount=1)]), 1),
        ([], 0),
        (iter([]), 0),
        ((row for row in []), 0),
    ],
)
def test_on_conflict_nothing_duplicate_rows(rows, expected_row_count):
    """Tests whether duplicate rows are filtered out when doing a insert
    NOTHING and no error is raised when the list of rows contains
    duplicates."""

    model = get_fake_model({"amount": models.IntegerField(unique=True)})

    inserted_rows = model.objects.on_conflict(
        ["amount"], ConflictAction.NOTHING
    ).bulk_insert(rows)

    assert len(inserted_rows) == expected_row_count
