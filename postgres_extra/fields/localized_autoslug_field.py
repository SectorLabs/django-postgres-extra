from typing import Callable

from django import forms
from django.conf import settings
from django.utils.text import slugify

from .localized_field import LocalizedField
from ..localized_value import LocalizedValue


class LocalizedAutoSlugField(LocalizedField):
    """Automatically provides slugs for a localized
    field upon saving."""

    def __init__(self, *args, **kwargs):
        """Initializes a new instance of :see:LocalizedAutoSlugField."""

        self.populate_from = kwargs.pop('populate_from', None)

        super(LocalizedAutoSlugField, self).__init__(
            *args,
            **kwargs
        )

    def deconstruct(self):
        """Deconstructs the field into something the database
        can store."""

        name, path, args, kwargs = super(
            LocalizedAutoSlugField, self).deconstruct()

        kwargs['populate_from'] = self.populate_from
        return name, path, args, kwargs

    def formfield(self, **kwargs):
        """Gets the form field associated with this field.

        Because this is a slug field which is automatically
        populated, it should be hidden from the form.
        """

        defaults = {
            'form_class': forms.CharField,
            'required': False
        }

        defaults.update(kwargs)

        form_field = super().formfield(**defaults)
        form_field.widget = forms.HiddenInput()

        return form_field

    def pre_save(self, instance, add: bool):
        """Ran just before the model is saved, allows us to built
        the slug.

        Arguments:
            instance:
                The model that is being saved.

            add:
                Indicates whether this is a new entry
                to the database or an update.
        """

        slugs = LocalizedValue()

        for lang_code, _ in settings.LANGUAGES:
            value = self._get_populate_from_value(
                instance,
                self.populate_from,
                lang_code
            )

            if not value:
                continue

            def is_unique(slug: str, language: str) -> bool:
                """Gets whether the specified slug is unique."""

                unique_filter = {
                    '%s__%s' % (self.name, language): slug
                }

                return not type(instance).objects.filter(**unique_filter).exists()

            slug = self._make_unique_slug(
                slugify(value, allow_unicode=True),
                lang_code,
                is_unique
            )

            slugs.set(lang_code, slug)

        setattr(instance, self.name, slugs)
        return slugs

    @staticmethod
    def _make_unique_slug(slug: str, language: str, is_unique: Callable[[str], bool]) -> str:
        """Guarentees that the specified slug is unique by appending
        a number until it is unique.

        Arguments:
            slug:
                The slug to make unique.

            is_unique:
                Function that can be called to verify
                whether the generate slug is unique.

        Returns:
            A guarenteed unique slug.
        """

        index = 1
        unique_slug = slug

        while not is_unique(unique_slug, language):
            unique_slug = '%s-%d' % (slug, index)
            index += 1

        return unique_slug

    @staticmethod
    def _get_populate_from_value(instance, field_name: str, language: str):
        """Gets the value to create a slug from in the specified language.

        Arguments:
            instance:
                The model that the field resides on.

            field_name:
                The name of the field to generate a slug for.

            language:
                The language to generate the slug for.

        Returns:
            The text to generate a slug for.
        """

        value = getattr(instance, field_name, None)
        return value.get(language)
