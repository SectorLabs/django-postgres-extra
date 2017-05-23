from django.db import models

from psqlextra.fields import HStoreField

from .util import get_fake_model


def test_upsert():
    """Tests whether simple upserts works correctly."""

    model = get_fake_model({
        'title': HStoreField(uniqueness=['key1']),
        'cookies': models.CharField(max_length=255, null=True)
    })

    obj1 = (
        model.objects
        .upsert_and_get(
            conflict_target=[('title', 'key1')],
            fields=dict(
                title={'key1': 'beer'},
                cookies='cheers'
            )
        )
    )

    obj1.refresh_from_db()
    assert obj1.title['key1'] == 'beer'
    assert obj1.cookies == 'cheers'

    obj2 = (
        model.objects
        .upsert_and_get(
            conflict_target=[('title', 'key1')],
            fields=dict(
                title={'key1': 'beer'},
                cookies='choco'
            )
        )
    )

    obj1.refresh_from_db()
    obj2.refresh_from_db()

    # assert both objects are the same
    assert obj1.id == obj2.id
    assert obj1.title['key1'] == 'beer'
    assert obj1.cookies == 'choco'
    assert obj2.title['key1'] == 'beer'
    assert obj2.cookies == 'choco'
