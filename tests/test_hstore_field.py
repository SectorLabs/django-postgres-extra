from psqlextra.fields import HStoreField


def test_deconstruct():
    """Tests whether the :see:HStoreField's deconstruct()
    method works properly."""

    original_kwargs = dict(uniqueness=['beer', 'other'])
    _, _, _, new_kwargs = HStoreField(**original_kwargs).deconstruct()

    for key, value in original_kwargs.items():
        assert new_kwargs[key] == value
