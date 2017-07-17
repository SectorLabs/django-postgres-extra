from django.db import models

from psqlextra.fields import HStoreField
from psqlextra.expressions import HStoreRef

from .fake_model import get_fake_model


def test_annotate_hstore_key_ref():
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
