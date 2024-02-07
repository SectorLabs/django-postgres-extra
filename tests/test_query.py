from datetime import datetime, timezone

from django.db import connection, models
from django.db.models import Case, F, Min, Q, Value, When
from django.db.models.functions.datetime import TruncSecond
from django.test.utils import CaptureQueriesContext, override_settings

from psqlextra.expressions import HStoreRef
from psqlextra.fields import HStoreField

from .fake_model import get_fake_model


def test_query_annotate_hstore_key_ref():
    """Tests whether annotating using a :see:HStoreRef expression works
    correctly.

    This allows you to select an individual hstore key.
    """

    model_fk = get_fake_model({"title": HStoreField()})

    model = get_fake_model(
        {"fk": models.ForeignKey(model_fk, on_delete=models.CASCADE)}
    )

    fk = model_fk.objects.create(title={"en": "english", "ar": "arabic"})
    model.objects.create(fk=fk)

    queryset = (
        model.objects.annotate(english_title=HStoreRef("fk__title", "en"))
        .values("english_title")
        .first()
    )

    assert queryset["english_title"] == "english"


def test_query_annotate_rename():
    """Tests whether field names can be overwritten with a annotated field."""

    model = get_fake_model({"title": models.CharField(max_length=12)})

    model.objects.create(title="swen")

    obj = model.objects.annotate(title=F("title")).first()
    assert obj.title == "swen"


def test_query_annotate_rename_chain():
    """Tests whether annotations are behaving correctly after a QuerySet
    chain."""

    model = get_fake_model(
        {
            "name": models.CharField(max_length=10),
            "value": models.IntegerField(),
        }
    )

    model.objects.create(name="test", value=23)

    obj = model.objects.values("name").annotate(value=F("value"))[:1]
    assert "value" in obj[0]
    assert obj[0]["value"] == 23


def test_query_annotate_rename_order():
    """Tests whether annotation order is preserved after a rename."""

    model = get_fake_model(
        {
            "name": models.CharField(max_length=10),
            "value": models.IntegerField(),
        }
    )

    qs = model.objects.annotate(value=F("value"), value_2=F("value"))
    assert list(qs.query.annotations.keys()) == ["value", "value_2"]


def test_query_annotate_in_expression():
    """Tests whether annotations can be used in expressions."""

    model = get_fake_model({"name": models.CharField(max_length=10)})

    model.objects.create(name="henk")

    result = model.objects.annotate(
        real_name=F("name"),
        is_he_henk=Case(
            When(Q(real_name="henk"), then=Value("really henk")),
            default=Value("definitely not henk"),
            output_field=models.CharField(),
        ),
    ).first()

    assert result.real_name == "henk"
    assert result.is_he_henk == "really henk"


def test_query_annotate_group_by():
    """Tests whether annotations with GROUP BY clauses are properly renamed
    when the annotation overwrites a field name."""

    model = get_fake_model(
        {
            "name": models.TextField(),
            "timestamp": models.DateTimeField(null=False),
            "value": models.IntegerField(),
        }
    )

    timestamp = datetime(2024, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)

    model.objects.create(name="me", timestamp=timestamp, value=1)

    result = (
        model.objects.values("name")
        .annotate(
            timestamp=TruncSecond("timestamp", tzinfo=timezone.utc),
            value=Min("value"),
        )
        .values_list(
            "name",
            "value",
            "timestamp",
        )
        .order_by("name")
        .first()
    )

    assert result == ("me", 1, timestamp)


def test_query_hstore_value_update_f_ref():
    """Tests whether F(..) expressions can be used in hstore values when
    performing update queries."""

    model = get_fake_model(
        {"name": models.CharField(max_length=255), "name_new": HStoreField()}
    )

    model.objects.create(name="waqas", name_new=dict(en="swen"))
    model.objects.update(name_new=dict(en=models.F("name")))

    inst = model.objects.all().first()
    assert inst.name_new.get("en") == "waqas"


def test_query_hstore_value_update_cast():
    """Tests whether values in a HStore field are automatically cast to strings
    when doing updates."""

    model = get_fake_model({"title": HStoreField()})

    model.objects.create(title=dict(en="test"))
    model.objects.update(title=dict(en=2))

    inst = model.objects.all().first()
    assert inst.title.get("en") == "2"


def test_query_hstore_value_update_escape():
    """Tests whether values in a HStore field are properly escaped using
    prepared statement values."""

    model = get_fake_model({"title": HStoreField()})

    model.objects.create(title=dict(en="test"))
    model.objects.update(title=dict(en="console.log('test')"))

    inst = model.objects.all().first()
    assert inst.title.get("en") == "console.log('test')"


@override_settings(POSTGRES_EXTRA_ANNOTATE_SQL=True)
def test_query_comment():
    """Tests whether the query is commented."""

    model = get_fake_model(
        {
            "name": models.CharField(max_length=10),
            "value": models.IntegerField(),
        }
    )

    with CaptureQueriesContext(connection) as queries:
        qs = model.objects.all()
        assert " test_query_comment " in str(qs.query)
        list(qs)
        assert " test_query_comment " in queries[0]["sql"]
