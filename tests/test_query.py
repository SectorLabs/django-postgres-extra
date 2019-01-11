from django.db import models

from psqlextra.fields import HStoreField
from psqlextra.expressions import HStoreRef

from django.db.models import F

from .fake_model import get_fake_model


def test_annotate_hstore_key_ref():
    """Tests whether annotating using a :see:HStoreRef expression
    works correctly.

    This allows you to select an individual hstore key."""

    model_fk = get_fake_model({
        'title': HStoreField(),
    })

    model = get_fake_model({
        'fk': models.ForeignKey(model_fk, on_delete=models.CASCADE)
    })

    fk = model_fk.objects.create(title={'en': 'english', 'ar': 'arabic'})
    model.objects.create(fk=fk)

    queryset = (
        model.objects
        .annotate(english_title=HStoreRef('fk__title', 'en'))
        .values('english_title')
        .first()
    )

    assert queryset['english_title'] == 'english'


def test_annotate_rename():
    """Tests whether field names can be overwritten
    with a annotated field."""

    model = get_fake_model({
        'title': models.CharField(max_length=12),
    })

    model.objects.create(title='swen')

    obj = model.objects.annotate(title=F('title')).first()
    assert obj.title == 'swen'


def test_hstore_f_ref():
    """Tests whether F(..) expressions can be used in
    hstore values when performing update queries."""

    model = get_fake_model({
        'name': models.CharField(max_length=255),
        'name_new': HStoreField()
    })

    model.objects.create(
        name='waqas',
        name_new=dict(en='swen')
    )

    model.objects.update(
        name_new=dict(en=models.F('name'))
    )

    inst = model.objects.all().first()
    assert inst.name_new.get('en') == 'waqas'
