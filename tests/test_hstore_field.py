import pytest

from psqlextra.fields import HStoreField


def test_deconstruct():
    """Tests whether the :see:HStoreField's deconstruct()
    method works properly."""

    original_kwargs = dict(uniqueness=['beer', 'other'], required=[])
    _, _, _, new_kwargs = HStoreField(**original_kwargs).deconstruct()

    for key, value in original_kwargs.items():
        assert new_kwargs[key] == value


@pytest.mark.parametrize('input,output', [
    (dict(key1=1, key2=2), dict(key1='1', key2='2')),
    (dict(key1='1', key2='2'), dict(key1='1', key2='2')),
    (dict(key1=1, key2=None, key3='3'), dict(key1='1', key2=None, key3='3')),
    ([1, 2, 3], ['1', '2', '3']),
    (['1', '2', '3'], ['1', '2', '3']),
])
def test_get_prep_value(input, output):
    """Tests whether the :see:HStoreField's get_prep_value
    method works properly."""

    assert HStoreField().get_prep_value(input) == output
