from typing import List, Tuple, Union

from django.contrib.postgres.fields import HStoreField as DjangoHStoreField
from django.db.models.expressions import Expression
from django.db.models.fields import Field

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

    def get_prep_value(self, value):
        """Override the base class so it doesn't cast all values
        to strings.

        psqlextra supports expressions in hstore fields, so casting
        all values to strings is a bad idea."""

        value = Field.get_prep_value(self, value)

        if isinstance(value, dict):
            prep_value = {}
            for key, val in value.items():
                if isinstance(val, Expression):
                    prep_value[key] = val
                elif val is not None:
                    prep_value[key] = str(val)
                else:
                    prep_value[key] = val

            value = prep_value

        if isinstance(value, list):
            value = [str(item) for item in value]

        return value

    def deconstruct(self):
        """Gets the values to pass to :see:__init__ when
        re-creating this object."""

        name, path, args, kwargs = super(
            HStoreField, self).deconstruct()

        if self.uniqueness is not None:
            kwargs['uniqueness'] = self.uniqueness

        if self.required is not None:
            kwargs['required'] = self.required

        return name, path, args, kwargs
