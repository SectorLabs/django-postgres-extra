from typing import List

from django import forms
from django.conf import settings
from django.forms import MultiWidget

from .localized_value import LocalizedValue


class LocalizedFieldWidget(MultiWidget):
    """Widget that has an input box for every language."""

    def __init__(self, *args, **kwargs):
        """Initializes a new instance of :see:LocalizedFieldWidget."""

        widgets = []

        for _ in settings.LANGUAGES:
            widgets.append(forms.Textarea())

        super(LocalizedFieldWidget, self).__init__(widgets, *args, **kwargs)

    def decompress(self, value: LocalizedValue) -> List[str]:
        """Decompresses the specified value so
        it can be spread over the internal widgets.

        Arguments:
            value:
                The :see:LocalizedValue to display in this
                widget.

        Returns:
            All values to display in the inner widgets.
        """

        result = []

        for lang_code, _ in settings.LANGUAGES:
            if value:
                result.append(value.get(lang_code))
            else:
                result.append(None)

        return result


class LocalizedFieldForm(forms.MultiValueField):
    """Form for a localized field, allows editing
    the field in multiple languages."""

    widget = LocalizedFieldWidget()

    def __init__(self, *args, **kwargs):
        """Initializes a new instance of :see:LocalizedFieldForm."""

        fields = []

        for lang_code, _ in settings.LANGUAGES:
            field_options = {'required': False}

            if lang_code == settings.LANGUAGE_CODE:
                field_options['required'] = True

            field_options['label'] = lang_code
            fields.append(forms.fields.CharField(**field_options))

        super(LocalizedFieldForm, self).__init__(
            fields,
            require_all_fields=False
        )

    def compress(self, value: List[str]) -> LocalizedValue:
        """Compresses the values from individual fields
        into a single :see:LocalizedValue instance.

        Arguments:
            value:
                The values from all the widgets.

        Returns:
            A :see:LocalizedValue containing all
            the value in several languages.
        """

        localized_value = LocalizedValue()

        for (lang_code, _), value in zip(settings.LANGUAGES, value):
            localized_value.set(lang_code, value)

        return localized_value
