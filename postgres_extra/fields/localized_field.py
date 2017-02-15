from django.conf import settings
from django.contrib.postgres.fields import HStoreField
from django.db.utils import IntegrityError

from ..forms import LocalizedFieldForm
from ..localized_value import LocalizedValue


class LocalizedField(HStoreField):
    """A field that has the same value in multiple languages.

    Internally this is stored as a :see:HStoreField where there
    is a key for every language."""

    Meta = None

    def __init__(self, *args, uniqueness=None, **kwargs):
        """Initializes a new instance of :see:LocalizedValue."""

        super(LocalizedField, self).__init__(*args, **kwargs)

        self.uniqueness = uniqueness

    @staticmethod
    def from_db_value(value, *_):
        """Turns the specified database value into its Python
        equivalent.

        Arguments:
            value:
                The value that is stored in the database and
                needs to be converted to its Python equivalent.

        Returns:
            A :see:LocalizedValue instance containing the
            data extracted from the database.
        """

        if not value:
            return LocalizedValue()

        return LocalizedValue(value)

    def to_python(self, value: dict) -> LocalizedValue:
        """Turns the specified database value into its Python
        equivalent.

        Arguments:
            value:
                The value that is stored in the database and
                needs to be converted to its Python equivalent.

        Returns:
            A :see:LocalizedValue instance containing the
            data extracted from the database.
        """

        if not value or not isinstance(value, dict):
            return LocalizedValue()

        return LocalizedValue(value)

    def get_prep_value(self, value: LocalizedValue) -> dict:
        """Turns the specified value into something the database
        can store.

        If an illegal value (non-LocalizedValue instance) is
        specified, we'll treat it as an empty :see:LocalizedValue
        instance, on which the validation will fail.

        Arguments:
            value:
                The :see:LocalizedValue instance to serialize
                into a data type that the database can understand.

        Returns:
            A dictionary containing a key for every language,
            extracted from the specified value.
        """

        # default to None if this is an unknown type
        if not isinstance(value, LocalizedValue) and value:
            value = None

        if value:
            cleaned_value = self.clean(value)
            self.validate(cleaned_value)
        else:
            cleaned_value = value

        return super(LocalizedField, self).get_prep_value(
            cleaned_value.__dict__ if cleaned_value else None
        )

    def clean(self, value, *_):
        """Cleans the specified value into something we
        can store in the database.

        For example, when all the language fields are
        left empty, and the field is allows to be null,
        we will store None instead of empty keys.

        Arguments:
            value:
                The value to clean.

        Returns:
            The cleaned value, ready for database storage.
        """

        if not value or not isinstance(value, LocalizedValue):
            return None

        # are any of the language fiels None/empty?
        is_all_null = True
        for lang_code, _ in settings.LANGUAGES:
            if value.get(lang_code):
                is_all_null = False
                break

        # all fields have been left empty and we support
        # null values, let's return null to represent that
        if is_all_null and self.null:
            return None

        return value

    def validate(self, value: LocalizedValue, *_):
        """Validates that the value for the primary language
        has been filled in.

        Exceptions are raises in order to notify the user
        of invalid values.

        Arguments:
            value:
                The value to validate.
        """

        if self.null:
            return

        primary_lang_val = getattr(value, settings.LANGUAGE_CODE)

        if not primary_lang_val:
            raise IntegrityError(
                'null value in column "%s.%s" violates not-null constraint' % (
                    self.name,
                    settings.LANGUAGE_CODE
                )
            )

    def formfield(self, **kwargs):
        """Gets the form field associated with this field."""

        defaults = {
            'form_class': LocalizedFieldForm
        }

        defaults.update(kwargs)
        return super().formfield(**defaults)

    def deconstruct(self):
        """Gets the values to pass to :see:__init__ when
        re-creating this object."""

        name, path, args, kwargs = super(
            LocalizedField, self).deconstruct()

        if self.uniqueness:
            kwargs['uniqueness'] = self.uniqueness

        return name, path, args, kwargs
