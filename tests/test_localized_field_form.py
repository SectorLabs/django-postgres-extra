from django.conf import settings
from django.test import TestCase

from postgres_extra import LocalizedFieldForm


class LocalizedFieldFormTestCase(TestCase):
    """Tests the workings of the :see:LocalizedFieldForm class."""

    @staticmethod
    def test_init():
        """Tests whether the constructor correctly
        creates a field for every language."""

        form = LocalizedFieldForm()

        for (lang_code, _), field in zip(settings.LANGUAGES, form.fields):
            assert field.label == lang_code

            if lang_code == settings.LANGUAGE_CODE:
                assert field.required
            else:
                assert not field.required

    @staticmethod
    def test_compress():
        """Tests whether the :see:compress function
        is working properly."""

        input_value = [lang_name for _, lang_name in settings.LANGUAGES]
        output_value = LocalizedFieldForm().compress(input_value)

        for lang_code, lang_name in settings.LANGUAGES:
            assert output_value.get(lang_code) == lang_name
