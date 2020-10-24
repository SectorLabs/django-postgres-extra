import uuid

from datetime import datetime

import freezegun

from dateutil.relativedelta import relativedelta
from django.db import models
from django.db.models import F, Sum

from psqlextra.expressions import HStoreRef
from psqlextra.fields import HStoreField
from psqlextra.types import PostgresPartitioningMethod

from .fake_model import get_fake_model, get_fake_partitioned_model


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


@freezegun.freeze_time("2020-10-23")
def test_query_annotate_rename_fail_for_warmwaffles():
    Account = get_fake_model(
        {"id": models.UUIDField(primary_key=True, default=uuid.uuid4)}
    )

    AmsCampaignReport = get_fake_partitioned_model(
        {
            "id": models.UUIDField(primary_key=True, default=uuid.uuid4),
            "account": models.ForeignKey(
                to=Account, on_delete=models.DO_NOTHING
            ),
            "report_date": models.DateField(),
            "metric": models.IntegerField(),
        },
        {"method": PostgresPartitioningMethod.RANGE, "key": ["report_date"]},
    )

    account1 = Account.objects.create()
    account2 = Account.objects.create()
    AmsCampaignReport.objects.create(
        account=account1, report_date=datetime.now().date(), metric=2
    )
    AmsCampaignReport.objects.create(
        account=account1, report_date=datetime.now().date(), metric=4
    )
    AmsCampaignReport.objects.create(
        account=account2, report_date=datetime.now().date(), metric=7
    )
    AmsCampaignReport.objects.create(
        account=account2, report_date=datetime.now().date(), metric=3
    )

    result = list(
        AmsCampaignReport.objects.filter(
            account__in=[account1, account2],
            report_date__range=(
                datetime.now() - relativedelta(days=1),
                datetime.now(),
            ),
        )
        .values("account_id", date=F("report_date"))
        .annotate(total_metric=Sum(F("metric")))
        .order_by("total_metric")
        .all()
    )

    assert result == [
        {
            "account_id": account1.id,
            "date": datetime.now().date(),
            "total_metric": 6,
        },
        {
            "account_id": account2.id,
            "date": datetime.now().date(),
            "total_metric": 10,
        },
    ]
