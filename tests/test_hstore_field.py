from django.test import TestCase
from enforce.exceptions import RuntimeTypeError
import pytest

from psqlextra import HStoreField


@pytest.mark.django_db
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

    def test_uniqueness_validation(self):
        """Tests whether the `uniqueness` option is properly validated."""

        with self.assertRaises(RuntimeTypeError):
            HStoreField(uniqueness='beer')

        with self.assertRaises(RuntimeTypeError):
            HStoreField(uniqueness=[12])

        with self.assertRaises(RuntimeTypeError):
            HStoreField(uniqueness=[('beer', 12)])

        HStoreField(uniqueness=['beer'])
        HStoreField(uniqueness=['beer', ('beer2', 'beer3')])

    def test_required_validation(self):
        """Tests whether the `required` option is properly validated."""

        with self.assertRaises(RuntimeTypeError):
            HStoreField(required='beer')

        with self.assertRaises(RuntimeTypeError):
            HStoreField(required=[12])

        with self.assertRaises(RuntimeTypeError):
            HStoreField(required=[('beer', 'beer1')])

        HStoreField(required=['beer'])
