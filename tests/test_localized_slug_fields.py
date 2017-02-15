from django import forms
from django.conf import settings
from django.test import TestCase
from django.db.utils import IntegrityError
from django.utils.text import slugify

from postgres_extra import (LocalizedField, LocalizedAutoSlugField,
                              LocalizedUniqueSlugField)

from .fake_model import get_fake_model


class LocalizedSlugFieldTestCase(TestCase):
    """Tests the localized slug classes."""

    AutoSlugModel = None
    MagicSlugModel = None

    @classmethod
    def setUpClass(cls):
        """Creates the test models in the database."""

        super(LocalizedSlugFieldTestCase, cls).setUpClass()

        cls.AutoSlugModel = get_fake_model(
            'LocalizedAutoSlugFieldTestModel',
            {
                'title': LocalizedField(),
                'slug': LocalizedAutoSlugField(populate_from='title')
            }
        )

        cls.MagicSlugModel = get_fake_model(
            'LocalizedUniqueSlugFieldTestModel',
            {
                'title': LocalizedField(),
                'slug': LocalizedUniqueSlugField(populate_from='title')
            }
        )

    @classmethod
    def test_populate_auto(cls):
        cls._test_populate(cls.AutoSlugModel)

    @classmethod
    def test_populate_magic(cls):
        cls._test_populate(cls.MagicSlugModel)

    @classmethod
    def test_populate_multiple_languages_auto(cls):
        cls._test_populate_multiple_languages(cls.AutoSlugModel)

    @classmethod
    def test_populate_multiple_languages_magic(cls):
        cls._test_populate_multiple_languages(cls.MagicSlugModel)

    @classmethod
    def test_unique_slug_auto(cls):
        cls._test_unique_slug(cls.AutoSlugModel)

    @classmethod
    def test_unique_slug_magic(cls):
        cls._test_unique_slug(cls.MagicSlugModel)

    def test_unique_slug_magic_max_retries(self):
        """Tests whether the magic slug implementation doesn't
        try to find a slug forever and gives up after a while."""

        title = 'mymagictitle'

        obj = self.MagicSlugModel()
        obj.title.en = title
        obj.save()

        with self.assertRaises(IntegrityError):
            for _ in range(0, settings.LOCALIZED_FIELDS_MAX_RETRIES + 1):
                another_obj = self.MagicSlugModel()
                another_obj.title.en = title
                another_obj.save()

    @classmethod
    def test_unique_slug_utf_auto(cls):
        cls._test_unique_slug_utf(cls.AutoSlugModel)

    @classmethod
    def test_unique_slug_utf_magic(cls):
        cls._test_unique_slug_utf(cls.MagicSlugModel)

    @classmethod
    def test_deconstruct_auto(cls):
        cls._test_deconstruct(LocalizedAutoSlugField)

    @classmethod
    def test_deconstruct_magic(cls):
        cls._test_deconstruct(LocalizedUniqueSlugField)

    @classmethod
    def test_formfield_auto(cls):
        cls._test_formfield(LocalizedAutoSlugField)

    @classmethod
    def test_formfield_magic(cls):
        cls._test_formfield(LocalizedUniqueSlugField)

    @staticmethod
    def _test_populate(model):
        """Tests whether the populating feature works correctly."""

        obj = model()
        obj.title.en = 'this is my title'
        obj.save()

        assert obj.slug.get('en') == slugify(obj.title)

    @staticmethod
    def _test_populate_multiple_languages(model):
        """Tests whether the populating feature correctly
        works for all languages."""

        obj = model()
        for lang_code, lang_name in settings.LANGUAGES:
            obj.title.set(lang_code, 'title %s' % lang_name)

        obj.save()

        for lang_code, lang_name in settings.LANGUAGES:
            assert obj.slug.get(lang_code) == 'title-%s' % lang_name.lower()

    @staticmethod
    def _test_unique_slug(model):
        """Tests whether unique slugs are properly generated."""

        title = 'mymagictitle'

        obj = model()
        obj.title.en = title
        obj.save()

        for i in range(1, settings.LOCALIZED_FIELDS_MAX_RETRIES - 1):
            another_obj = model()
            another_obj.title.en = title
            another_obj.save()

            assert another_obj.slug.en == '%s-%d' % (title, i)

    @staticmethod
    def _test_unique_slug_utf(model):
        """Tests whether generating a slug works
        when the value consists completely out
        of non-ASCII characters."""

        obj = model()
        obj.title.en = 'مكاتب للايجار بشارع بورسعيد'
        obj.save()

        assert obj.slug.en == 'مكاتب-للايجار-بشارع-بورسعيد'

    @staticmethod
    def _test_deconstruct(field_type):
        """Tests whether the :see:deconstruct
        function properly retains options
        specified in the constructor."""

        field = field_type(populate_from='title')
        _, _, _, kwargs = field.deconstruct()

        assert 'populate_from' in kwargs
        assert kwargs['populate_from'] == field.populate_from

    @staticmethod
    def _test_formfield(field_type):
        """Tests whether the :see:formfield method
        returns a valid form field that is hidden."""

        form_field = field_type(populate_from='title').formfield()

        assert isinstance(form_field, forms.CharField)
        assert isinstance(form_field.widget, forms.HiddenInput)
