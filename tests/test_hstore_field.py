from django.db import models

from psqlextra import HStoreField
from psqlextra.expressions import HStoreRef

from .fake_model import get_fake_model


def test_deconstruct():
    """Tests whether the :see:HStoreField's deconstruct()
    method works properly."""

    original_kwargs = dict(uniqueness=['beer', 'other'])
    _, _, _, new_kwargs = HStoreField(**original_kwargs).deconstruct()

    for key, value in original_kwargs.items():
        assert new_kwargs[key] == value


def test_values():
    """Tests whether selecting only certain hstore keys using the
    query set's .values() method works properly."""

    model_fk = get_fake_model({
        'first_name': models.CharField(max_length=255),
        'last_name': models.CharField(max_length=255)
    })

    model = get_fake_model({
        'title': HStoreField(),
        'fk': models.ForeignKey(model_fk)
    })

    fk = model_fk.objects.create(first_name='Swen', last_name='Kooij')
    obj = model.objects.create(title={'en': 'english', 'ar': 'arabic'}, fk=fk)

    # ensure that selecting only certain keys from a hstore field works
    result = list(model.objects.values('title__en', 'fk__first_name', 'title__ar'))[0]
    assert result['title__en'] == obj.title['en']
    assert result['title__ar'] == obj.title['ar']

    # make sure that selecting the whole hstore field works properly
    result = list(model.objects.values('fk__first_name', 'title'))[0]
    assert result['title'] == obj.title

    # make sure .values_list() also works properly
    result = list(model.objects.values_list('title__en', 'title__ar'))[0]
    assert result[0] == obj.title['en']
    assert result[1] == obj.title['ar']

    result = list(model.objects.values_list('title__en', 'title__ar'))[0]


def test_annotate_ref():
    """Tests whether annotating using a :see:HStoreRef expression
    works correctly.

    This allows you to select an individual hstore key."""

    model_fk = get_fake_model({
        'title': HStoreField(),
    })

    model = get_fake_model({
        'fk': models.ForeignKey(model_fk)
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
