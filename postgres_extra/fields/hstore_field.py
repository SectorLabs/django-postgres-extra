from django.contrib.postgres.fields import HStoreField as DjangoHStoreField


class HStoreField(DjangoHStoreField):
    """Improved version of Django's :see:HStoreField that
    adds support for database-level constraints.

    Notes:
        - For the implementation of uniqueness, see the
          custom database back-end.
    """

    def __init__(self, *args, uniqueness=None, **kwargs):
        """Initializes a new instance of :see:HStoreField."""

        super(HStoreField, self).__init__(*args, **kwargs)

        self.uniqueness = uniqueness

    def deconstruct(self):
        """Gets the values to pass to :see:__init__ when
        re-creating this object."""

        name, path, args, kwargs = super(
            HStoreField, self).deconstruct()

        if self.uniqueness:
            kwargs['uniqueness'] = self.uniqueness

        return name, path, args, kwargs
