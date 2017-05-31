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
