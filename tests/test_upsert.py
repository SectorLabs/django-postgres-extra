import django
import pytest

from django.db import models
from django.db.models import Q
from django.db.models.expressions import CombinedExpression, Value

from psqlextra.expressions import ExcludedCol
from psqlextra.fields import HStoreField

from .fake_model import get_fake_model


def test_upsert():
    """Tests whether simple upserts works correctly."""

    model = get_fake_model(
        {
            "title": HStoreField(uniqueness=["key1"]),
            "cookies": models.CharField(max_length=255, null=True),
        }
    )

    obj1 = model.objects.upsert_and_get(
        conflict_target=[("title", "key1")],
        fields=dict(title={"key1": "beer"}, cookies="cheers"),
    )

    obj1.refresh_from_db()
    assert obj1.title["key1"] == "beer"
    assert obj1.cookies == "cheers"

    obj2 = model.objects.upsert_and_get(
        conflict_target=[("title", "key1")],
        fields=dict(title={"key1": "beer"}, cookies="choco"),
    )

    obj1.refresh_from_db()
    obj2.refresh_from_db()

    # assert both objects are the same
    assert obj1.id == obj2.id
    assert obj1.title["key1"] == "beer"
    assert obj1.cookies == "choco"
    assert obj2.title["key1"] == "beer"
    assert obj2.cookies == "choco"


def test_upsert_explicit_pk():
    """Tests whether upserts works when the primary key is explicitly
    specified."""

    model = get_fake_model(
        {
            "name": models.CharField(max_length=255, primary_key=True),
            "cookies": models.CharField(max_length=255, null=True),
        }
    )

    obj1 = model.objects.upsert_and_get(
        conflict_target=[("name")],
        fields=dict(name="the-object", cookies="first-cheers"),
    )

    obj1.refresh_from_db()
    assert obj1.name == "the-object"
    assert obj1.cookies == "first-cheers"

    obj2 = model.objects.upsert_and_get(
        conflict_target=[("name")],
        fields=dict(name="the-object", cookies="second-boo"),
    )

    obj1.refresh_from_db()
    obj2.refresh_from_db()

    # assert both objects are the same
    assert obj1.pk == obj2.pk
    assert obj1.name == "the-object"
    assert obj1.cookies == "second-boo"
    assert obj2.name == "the-object"
    assert obj2.cookies == "second-boo"


def test_upsert_with_update_condition():
    """Tests that an expression can be used as an upsert update condition."""

    model = get_fake_model(
        {
            "name": models.TextField(unique=True),
            "priority": models.IntegerField(),
            "active": models.BooleanField(),
        }
    )

    obj1 = model.objects.create(name="joe", priority=1, active=False)

    # should not return anything because no rows were affected
    assert not model.objects.upsert(
        conflict_target=["name"],
        update_condition=CombinedExpression(
            model._meta.get_field("active").get_col(model._meta.db_table),
            "=",
            ExcludedCol("active"),
        ),
        fields=dict(name="joe", priority=2, active=True),
    )

    obj1.refresh_from_db()
    assert obj1.priority == 1
    assert not obj1.active

    # should return something because one row was affected
    obj1_pk = model.objects.upsert(
        conflict_target=["name"],
        update_condition=CombinedExpression(
            model._meta.get_field("active").get_col(model._meta.db_table),
            "=",
            Value(False),
        ),
        fields=dict(name="joe", priority=2, active=True),
    )

    obj1.refresh_from_db()
    assert obj1.pk == obj1_pk
    assert obj1.priority == 2
    assert obj1.active


@pytest.mark.skipif(
    django.VERSION < (3, 1), reason="requires django 3.1 or newer"
)
def test_upsert_with_update_condition_with_q_object():
    """Tests that :see:Q objects can be used as an upsert update condition."""

    model = get_fake_model(
        {
            "name": models.TextField(unique=True),
            "priority": models.IntegerField(),
            "active": models.BooleanField(),
        }
    )

    obj1 = model.objects.create(name="joe", priority=1, active=False)

    # should not return anything because no rows were affected
    assert not model.objects.upsert(
        conflict_target=["name"],
        update_condition=Q(active=ExcludedCol("active")),
        fields=dict(name="joe", priority=2, active=True),
    )

    obj1.refresh_from_db()
    assert obj1.priority == 1
    assert not obj1.active

    # should return something because one row was affected
    obj1_pk = model.objects.upsert(
        conflict_target=["name"],
        update_condition=Q(active=Value(False)),
        fields=dict(name="joe", priority=2, active=True),
    )

    obj1.refresh_from_db()
    assert obj1.pk == obj1_pk
    assert obj1.priority == 2
    assert obj1.active


def test_upsert_and_get_applies_converters():
    """Tests that converters are properly applied when using upsert_and_get."""

    class MyCustomField(models.TextField):
        def from_db_value(self, value, expression, connection):
            return value.replace("hello", "bye")

    model = get_fake_model({"title": MyCustomField(unique=True)})

    obj = model.objects.upsert_and_get(
        conflict_target=["title"], fields=dict(title="hello")
    )

    assert obj.title == "bye"


def test_upsert_bulk():
    """Tests whether bulk_upsert works properly."""

    model = get_fake_model(
        {
            "first_name": models.CharField(
                max_length=255, null=True, unique=True
            ),
            "last_name": models.CharField(max_length=255, null=True),
        }
    )

    model.objects.bulk_upsert(
        conflict_target=["first_name"],
        rows=[
            dict(first_name="Swen", last_name="Kooij"),
            dict(first_name="Henk", last_name="Test"),
        ],
    )

    row_a = model.objects.get(first_name="Swen")
    row_b = model.objects.get(first_name="Henk")

    model.objects.bulk_upsert(
        conflict_target=["first_name"],
        rows=[
            dict(first_name="Swen", last_name="Test"),
            dict(first_name="Henk", last_name="Kooij"),
        ],
    )

    row_a.refresh_from_db()
    assert row_a.last_name == "Test"

    row_b.refresh_from_db()
    assert row_b.last_name == "Kooij"


def test_upsert_bulk_no_rows():
    """Tests whether bulk_upsert doesn't crash when specifying no rows or a
    falsy value."""

    model = get_fake_model(
        {"name": models.CharField(max_length=255, null=True, unique=True)}
    )

    model.objects.bulk_upsert(conflict_target=["name"], rows=[])

    model.objects.bulk_upsert(conflict_target=["name"], rows=None)


def test_bulk_upsert_return_models():
    """Tests whether models are returned instead of dictionaries when
    specifying the return_model=True argument."""

    model = get_fake_model(
        {
            "id": models.BigAutoField(primary_key=True),
            "name": models.CharField(max_length=255, unique=True),
        }
    )

    rows = [dict(name="John Smith"), dict(name="Jane Doe")]

    objs = model.objects.bulk_upsert(
        conflict_target=["name"], rows=rows, return_model=True
    )

    for index, obj in enumerate(objs, 1):
        assert isinstance(obj, model)
        assert obj.id == index


def test_bulk_upsert_accepts_getitem_iterable():
    """Tests whether an iterable only implementing the __getitem__ method works
    correctly."""

    class GetItemIterable:
        def __init__(self, items):
            self.items = items

        def __getitem__(self, key):
            return self.items[key]

    model = get_fake_model(
        {
            "id": models.BigAutoField(primary_key=True),
            "name": models.CharField(max_length=255, unique=True),
        }
    )

    rows = GetItemIterable([dict(name="John Smith"), dict(name="Jane Doe")])

    objs = model.objects.bulk_upsert(
        conflict_target=["name"], rows=rows, return_model=True
    )

    for index, obj in enumerate(objs, 1):
        assert isinstance(obj, model)
        assert obj.id == index


def test_bulk_upsert_accepts_iter_iterable():
    """Tests whether an iterable only implementing the __iter__ method works
    correctly."""

    class IterIterable:
        def __init__(self, items):
            self.items = items

        def __iter__(self):
            return iter(self.items)

    model = get_fake_model(
        {
            "id": models.BigAutoField(primary_key=True),
            "name": models.CharField(max_length=255, unique=True),
        }
    )

    rows = IterIterable([dict(name="John Smith"), dict(name="Jane Doe")])

    objs = model.objects.bulk_upsert(
        conflict_target=["name"], rows=rows, return_model=True
    )

    for index, obj in enumerate(objs, 1):
        assert isinstance(obj, model)
        assert obj.id == index
