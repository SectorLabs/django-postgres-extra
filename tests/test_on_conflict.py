import django
import freezegun
import pytest

from django.core.exceptions import SuspiciousOperation
from django.db import connection, models
from django.test.utils import CaptureQueriesContext, override_settings
from django.utils import timezone

from psqlextra.fields import HStoreField
from psqlextra.models import PostgresModel
from psqlextra.query import ConflictAction

from .fake_model import get_fake_model


@pytest.mark.parametrize("conflict_action", ConflictAction.all())
@override_settings(POSTGRES_EXTRA_ANNOTATE_SQL=True)
def test_on_conflict(conflict_action):
    """Tests whether simple inserts work correctly."""

    model = get_fake_model(
        {
            "title": HStoreField(uniqueness=["key1"]),
            "cookies": models.CharField(max_length=255, null=True),
        }
    )

    with CaptureQueriesContext(connection) as queries:
        obj = model.objects.on_conflict(
            [("title", "key1")], conflict_action
        ).insert_and_get(title={"key1": "beer"}, cookies="cheers")
        assert " test_on_conflict " in queries[0]["sql"]

    model.objects.on_conflict(
        [("title", "key1")], conflict_action
    ).insert_and_get(title={"key1": "beer"})

    assert model.objects.count() == 1

    # make sure the data is actually in the db
    obj.refresh_from_db()
    assert obj.title["key1"] == "beer"
    assert obj.cookies == "cheers"


def test_on_conflict_auto_fields():
    """Asserts that fields that automatically add something to the model
    automatically still work properly when upserting."""

    model = get_fake_model(
        {
            "title": models.CharField(max_length=255, unique=True),
            "date_added": models.DateTimeField(auto_now_add=True),
            "date_updated": models.DateTimeField(auto_now=True),
        }
    )

    obj1 = model.objects.on_conflict(
        ["title"], ConflictAction.UPDATE
    ).insert_and_get(title="beer")

    obj2 = model.objects.on_conflict(
        ["title"], ConflictAction.UPDATE
    ).insert_and_get(title="beer")

    obj2.refresh_from_db()

    assert obj1.date_added
    assert obj2.date_added

    assert obj1.date_updated
    assert obj2.date_updated

    assert obj1.id == obj2.id
    assert obj1.title == obj2.title
    assert obj1.date_added == obj2.date_added
    assert obj1.date_updated != obj2.date_updated


@pytest.mark.parametrize("conflict_action", ConflictAction.all())
def test_on_conflict_foreign_key(conflict_action):
    """Asserts that models with foreign key relationships can safely be
    inserted."""

    model1 = get_fake_model(
        {"name": models.CharField(max_length=255, unique=True)}
    )

    model2 = get_fake_model(
        {
            "name": models.CharField(max_length=255, unique=True),
            "model1": models.ForeignKey(model1, on_delete=models.CASCADE),
        }
    )

    model1_row = model1.objects.on_conflict(
        ["name"], conflict_action
    ).insert_and_get(name="item1")

    # insert by id, that should work
    model2_row = model2.objects.on_conflict(
        ["name"], conflict_action
    ).insert_and_get(name="item1", model1_id=model1_row.id)

    model2_row = model2.objects.get(name="item1")
    assert model2_row.name == "item1"
    assert model2_row.model1.id == model1_row.id

    # insert by object, that should also work
    model2_row = model2.objects.on_conflict(
        ["name"], conflict_action
    ).insert_and_get(name="item2", model1=model1_row)

    model2_row.refresh_from_db()

    assert model2_row.name == "item2"
    assert model2_row.model1.id == model1_row.id


def test_on_conflict_partial_get():
    """Asserts that when doing a insert_and_get with only part of the columns
    on the model, all fields are returned properly."""

    model = get_fake_model(
        {
            "title": models.CharField(max_length=140, unique=True),
            "purpose": models.CharField(max_length=10, null=True),
            "created_at": models.DateTimeField(auto_now_add=True),
            "updated_at": models.DateTimeField(auto_now=True),
        }
    )

    with freezegun.freeze_time("2020-1-1 12:00:00.0") as fg:
        obj1 = model.objects.on_conflict(
            ["title"], ConflictAction.UPDATE
        ).insert_and_get(title="beer", purpose="for-sale")

        fg.tick()

        obj2 = model.objects.on_conflict(
            ["title"], ConflictAction.UPDATE
        ).insert_and_get(title="beer")

    obj2.refresh_from_db()

    assert obj2.title == obj1.title
    assert obj2.purpose == obj1.purpose
    assert obj2.created_at == obj2.created_at
    assert obj1.updated_at != obj2.updated_at


@pytest.mark.parametrize("conflict_action", ConflictAction.all())
def test_on_conflict_invalid_target(conflict_action):
    """Tests whether specifying a invalid value for `conflict_target` raises an
    error."""

    model = get_fake_model(
        {"title": models.CharField(max_length=140, unique=True)}
    )

    with pytest.raises(SuspiciousOperation):
        (
            model.objects.on_conflict(["cookie"], conflict_action).insert(
                title="beer"
            )
        )

    with pytest.raises(SuspiciousOperation):
        (
            model.objects.on_conflict([None], conflict_action).insert(
                title="beer"
            )
        )


@pytest.mark.parametrize("conflict_action", ConflictAction.all())
def test_on_conflict_outdated_model(conflict_action):
    """Tests whether insert properly handles fields that are in the database
    but not on the model.

    This happens if somebody manually modified the database to add a
    column that is not present in the model.

    This should be handled properly by ignoring the column returned by
    the database.
    """

    model = get_fake_model(
        {"title": models.CharField(max_length=140, unique=True)}
    )

    # manually create the colum that is not on the model
    with connection.cursor() as cursor:
        cursor.execute(
            (
                "ALTER TABLE {table} " "ADD COLUMN beer character varying(50);"
            ).format(table=model._meta.db_table)
        )

    # without proper handling, this would fail with a TypeError
    (
        model.objects.on_conflict(["title"], conflict_action).insert_and_get(
            title="beer"
        )
    )


@pytest.mark.parametrize("conflict_action", ConflictAction.all())
def test_on_conflict_custom_column_names(conflict_action):
    """Asserts that models with custom column names (models where the column
    and field name are different) work properly."""

    model = get_fake_model(
        {
            "title": models.CharField(
                max_length=140, unique=True, db_column="beer"
            ),
            "description": models.CharField(max_length=255, db_column="desc"),
        }
    )

    (
        model.objects.on_conflict(["title"], conflict_action).insert(
            title="yeey", description="great thing"
        )
    )


def test_on_conflict_unique_together():
    """Asserts that inserts on models with a unique_together works properly."""

    model = get_fake_model(
        {
            "first_name": models.CharField(max_length=140),
            "last_name": models.CharField(max_length=255),
        },
        PostgresModel,
        {"unique_together": ("first_name", "last_name")},
    )

    id1 = model.objects.on_conflict(
        ["first_name", "last_name"], ConflictAction.UPDATE
    ).insert(first_name="swen", last_name="kooij")

    id2 = model.objects.on_conflict(
        ["first_name", "last_name"], ConflictAction.UPDATE
    ).insert(first_name="swen", last_name="kooij")

    assert id1 == id2


def test_on_conflict_unique_together_fk():
    """Asserts that inserts on models with a unique_together and a foreign key
    relationship works properly."""

    model = get_fake_model({"name": models.CharField(max_length=140)})

    model2 = get_fake_model(
        {
            "model1": models.ForeignKey(model, on_delete=models.CASCADE),
            "model2": models.ForeignKey(model, on_delete=models.CASCADE),
        },
        PostgresModel,
        {"unique_together": ("model1", "model2")},
    )

    id1 = model.objects.create(name="one").id
    id2 = model.objects.create(name="two").id

    assert id1 != id2

    id3 = model2.objects.on_conflict(
        ["model1_id", "model2_id"], ConflictAction.UPDATE
    ).insert(model1_id=id1, model2_id=id2)

    id4 = model2.objects.on_conflict(
        ["model1_id", "model2_id"], ConflictAction.UPDATE
    ).insert(model1_id=id1, model2_id=id2)

    assert id3 == id4


def test_on_conflict_pk_conflict_target():
    """Tests whether `on_conflict` properly accepts the 'pk' as a conflict
    target, which should resolve into the primary key of a model."""

    model = get_fake_model({"name": models.CharField(max_length=255)})

    obj1 = model.objects.on_conflict(
        ["pk"], ConflictAction.UPDATE
    ).insert_and_get(pk=0, name="beer")

    obj2 = model.objects.on_conflict(
        ["pk"], ConflictAction.UPDATE
    ).insert_and_get(pk=0, name="beer")

    assert obj1.name == "beer"
    assert obj2.name == "beer"
    assert obj1.id == obj2.id
    assert obj1.id == 0
    assert obj2.id == 0


def test_on_conflict_default_value():
    """Tests whether setting a default for a field and not specifying it
    explicitely when upserting properly causes the default value to be used."""

    model = get_fake_model(
        {"title": models.CharField(max_length=255, default="great")}
    )

    obj1 = model.objects.on_conflict(
        ["id"], ConflictAction.UPDATE
    ).insert_and_get(id=0)

    assert obj1.title == "great"

    obj2 = model.objects.on_conflict(
        ["id"], ConflictAction.UPDATE
    ).insert_and_get(id=0)

    assert obj1.id == obj2.id
    assert obj2.title == "great"


def test_on_conflict_default_value_no_overwrite():
    """Tests whether setting a default for a field, inserting a non-default
    value and then trying to update it without specifying that field doesn't
    result in it being overwritten."""

    model = get_fake_model(
        {"title": models.CharField(max_length=255, default="great")}
    )

    obj1 = model.objects.on_conflict(
        ["id"], ConflictAction.UPDATE
    ).insert_and_get(id=0, title="mytitle")

    assert obj1.title == "mytitle"

    obj2 = model.objects.on_conflict(
        ["id"], ConflictAction.UPDATE
    ).insert_and_get(id=0)

    assert obj1.id == obj2.id
    assert obj2.title == "mytitle"


def test_on_conflict_bulk():
    """Tests whether using `on_conflict` with `insert_bulk` properly works."""

    model = get_fake_model(
        {"title": models.CharField(max_length=255, unique=True)}
    )

    rows = [
        dict(title="this is my title"),
        dict(title="this is another title"),
        dict(title="and another one"),
    ]

    (
        model.objects.on_conflict(["title"], ConflictAction.UPDATE).bulk_insert(
            rows
        )
    )

    assert model.objects.all().count() == len(rows)

    for index, obj in enumerate(list(model.objects.all())):
        assert obj.title == rows[index]["title"]


def test_bulk_return():
    """Tests if primary keys are properly returned from 'bulk_insert'."""

    model = get_fake_model(
        {
            "id": models.BigAutoField(primary_key=True),
            "name": models.CharField(max_length=255, unique=True),
        }
    )

    rows = [dict(name="John Smith"), dict(name="Jane Doe")]

    objs = model.objects.on_conflict(
        ["name"], ConflictAction.UPDATE
    ).bulk_insert(rows)

    for index, obj in enumerate(objs, 1):
        assert obj["id"] == index

    # Add objects again, update should return the same ids
    # as we're just updating.
    objs = model.objects.on_conflict(
        ["name"], ConflictAction.UPDATE
    ).bulk_insert(rows)

    for index, obj in enumerate(objs, 1):
        assert obj["id"] == index


@pytest.mark.parametrize("conflict_action", ConflictAction.all())
def test_bulk_return_models(conflict_action):
    """Tests whether models are returned instead of dictionaries when
    specifying the return_model=True argument."""

    model = get_fake_model(
        {
            "id": models.BigAutoField(primary_key=True),
            "name": models.CharField(max_length=255, unique=True),
        }
    )

    rows = [dict(name="John Smith"), dict(name="Jane Doe")]

    objs = model.objects.on_conflict(["name"], conflict_action).bulk_insert(
        rows, return_model=True
    )

    for index, obj in enumerate(objs, 1):
        assert isinstance(obj, model)
        assert obj.id == index


@pytest.mark.skipif(
    django.VERSION < (3, 1),
    reason="Django < 3.1 doesn't implement JSONField",
)
@pytest.mark.parametrize("conflict_action", ConflictAction.all())
def test_bulk_return_models_converters(conflict_action):
    """Tests whether converters are properly applied when using
    return_model=True."""

    model = get_fake_model(
        {
            "name": models.TextField(unique=True),
            "data": models.JSONField(unique=True),
            "updated_at": models.DateTimeField(),
        }
    )

    now = timezone.now()

    rows = [
        dict(name="John Smith", data={"a": 1}, updated_at=now.isoformat()),
        dict(name="Jane Doe", data={"b": 2}, updated_at=now),
    ]

    objs = model.objects.on_conflict(["name"], conflict_action).bulk_insert(
        rows, return_model=True
    )

    for index, (obj, row) in enumerate(zip(objs, rows), 1):
        assert isinstance(obj, model)
        assert obj.id == index
        assert obj.name == row["name"]
        assert obj.data == row["data"]
        assert obj.updated_at == now
