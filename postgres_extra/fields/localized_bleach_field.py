import bleach
from django.conf import settings
from django_bleach.utils import get_bleach_default_options

from .localized_field import LocalizedField


class LocalizedBleachField(LocalizedField):
    """Custom version of :see:BleachField that
    is actually a :see:LocalizedField."""

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

        localized_value = getattr(instance, self.attname)
        if not localized_value:
            return None

        for lang_code, _ in settings.LANGUAGES:
            value = localized_value.get(lang_code)
            if not value:
                continue

            localized_value.set(
                lang_code,
                bleach.clean(value, **get_bleach_default_options())
            )

        return localized_value
