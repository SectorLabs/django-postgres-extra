from django.conf import settings
from django.test import TestCase
from django_bleach.utils import get_bleach_default_options
import bleach

from postgres_extra import LocalizedBleachField, LocalizedValue


class TestModel:
    """Used to declare a bleach-able field on."""

    def __init__(self, value):
        """Initializes a new instance of :see:TestModel.

        Arguments:
            The value to initialize with.
        """

        self.value = value


class LocalizedBleachFieldTestCase(TestCase):
    """Tests the :see:LocalizedBleachField class."""

    def test_pre_save(self):
        """Tests whether the :see:pre_save function
        bleaches all values in a :see:LocalizedValue."""

        value = self._get_test_value()
        model, field = self._get_test_model(value)

        bleached_value = field.pre_save(model, False)
        self._validate(value, bleached_value)

    def test_pre_save_none(self):
        """Tests whether the :see:pre_save function
        works properly when specifying :see:None."""

        model, field = self._get_test_model(None)

        bleached_value = field.pre_save(model, False)
        assert not bleached_value

    def test_pre_save_none_values(self):
        """Tests whether the :see:pre_save function
        works properly when one of the languages has
        no text and is None."""

        value = self._get_test_value()
        value.set(settings.LANGUAGE_CODE, None)

        model, field = self._get_test_model(value)

        bleached_value = field.pre_save(model, False)
        self._validate(value, bleached_value)

    @staticmethod
    def _get_test_model(value):
        """Gets a test model and a artifically
        constructed :see:LocalizedBleachField
        instance to test with."""

        model = TestModel(value)

        field = LocalizedBleachField()
        field.attname = 'value'
        return model, field

    @staticmethod
    def _get_test_value():
        """Gets a :see:LocalizedValue instance for testing."""

        value = LocalizedValue()

        for lang_code, lang_name in settings.LANGUAGES:
            value.set(lang_code, '<script>%s</script>' % lang_name)

        return value

    @staticmethod
    def _validate(non_bleached_value, bleached_value):
        """Validates whether the specified non-bleached
        value ended up being correctly bleached.

        Arguments:
            non_bleached_value:
                The value before bleaching.

            bleached_value:
                The value after bleaching.
        """

        for lang_code, _ in settings.LANGUAGES:
            if not non_bleached_value.get(lang_code):
                assert not bleached_value.get(lang_code)
                continue

            expected_value = bleach.clean(
                non_bleached_value.get(lang_code),
                get_bleach_default_options()
            )

            assert bleached_value.get(lang_code) == expected_value
