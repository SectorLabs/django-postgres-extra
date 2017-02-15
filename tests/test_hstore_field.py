from django.test import TestCase

from postgres_extra import HStoreField


class HStoreFieldTestCase(TestCase):
    """Tests the :see:HStoreField class."""

    @staticmethod
    def test_deconstruct():
        """Tests whether the :see:HStoreField's deconstruct()
        method works properly."""

        original_kwargs = dict(uniqueness=['beer', 'other'])
        _, _, _, new_kwargs = HStoreField(**original_kwargs).deconstruct()

        for key, value in original_kwargs.items():
            assert new_kwargs[key] == value
