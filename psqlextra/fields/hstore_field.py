from typing import List, Tuple, Union

from django.contrib.postgres.fields import HStoreField as DjangoHStoreField

from psqlextra.expressions import HStoreValue


class HStoreField(DjangoHStoreField):
    """Improved version of Django's :see:HStoreField that
    adds support for database-level constraints.

    Notes:
        - For the implementation of uniqueness, see the
          custom database back-end.
    """

    def __init__(self, *args,
                 uniqueness: List[Union[str, Tuple[str, ...]]]=None,
                 required: List[str]=None, **kwargs):
        """Initializes a new instance of :see:HStoreField."""

        super(HStoreField, self).__init__(*args, **kwargs)

        self.uniqueness = uniqueness
        self.required = required

    def get_db_prep_value(self, value, connection, prepared=False):
        """Override the base class so it doesn't cast all values
        to strings.

        psqlextra supports expressions in hstore fields, so casting
        all values to strings is a bad idea."""

        if not value:
            return None

        return value

    def deconstruct(self):
        """Gets the values to pass to :see:__init__ when
        re-creating this object."""

        name, path, args, kwargs = super(
            HStoreField, self).deconstruct()

        kwargs['uniqueness'] = self.uniqueness or []
        kwargs['required'] = self.required or []

        return name, path, args, kwargs
