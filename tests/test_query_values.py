import pytest

from django.db.models import ForeignKey

from psqlextra.fields import HStoreField

from .fake_model import get_fake_model


@pytest.fixture
def model():
    """Test models, where the first model has a foreign
    key relationship to the second."""

    return get_fake_model({
        'title': HStoreField(),
    })


@pytest.fixture
def modelobj(model):
    """Data for the test models, one row per model."""

    return model.objects.create(title={'en': 'english', 'ar': 'arabic'})


def test_values_hstore(model, modelobj):
    """Tests that selecting all the keys properly works
    and returns a :see:LocalizedValue instance."""

    result = list(model.objects.values('title'))[0]
    assert result['title'] == modelobj.title


def test_values_hstore_key(model, modelobj):
    """Tests whether selecting a single key from a :see:HStoreField
    using the query set's .values() works properly."""

    result = list(model.objects.values('title__en', 'title__ar'))[0]
    assert result['title__en'] == modelobj.title['en']
    assert result['title__ar'] == modelobj.title['ar']


def test_values_list_hstore_key(model, modelobj):
    """Tests that selecting a single key from a :see:HStoreField
    using the query set's .values_list() works properly."""

    result = list(model.objects.values_list('title__en', 'title__ar'))[0]
    assert result[0] == modelobj.title['en']
    assert result[1] == modelobj.title['ar']


@pytest.mark.xfail(reason='has to be fixed as part of issue #8')
def test_values_hstore_key_through_fk():
    """Tests whether selecting a single key from a :see:HStoreField
    using the query set's .values() works properly when there's a
    foreign key relationship involved."""

    fmodel = get_fake_model({
        'name': HStoreField()
    })

    model = get_fake_model({
        'fk': ForeignKey(fmodel)
    })

    fobj = fmodel.objects.create(name={'en': 'swen', 'ar': 'arabic swen'})
    model.objects.create(fk=fobj)

    result = list(model.objects.values('fk__name__ar'))[0]
    assert result['fk__name__ar'] == fobj.name['ar']
