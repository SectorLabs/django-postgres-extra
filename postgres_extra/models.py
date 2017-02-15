from django.db import models, transaction
from django.db.utils import IntegrityError
from django.conf import settings

from .fields import LocalizedField
from .localized_value import LocalizedValue


class LocalizedModel(models.Model):
    """A model that contains localized fields."""

    class Meta:
        abstract = True

    def __init__(self, *args, **kwargs):
        """Initializes a new instance of :see:LocalizedModel.

        Here we set all the fields that are of :see:LocalizedField
        to an instance of :see:LocalizedValue in case they are none
        so that the user doesn't explicitely have to do so."""

        super(LocalizedModel, self).__init__(*args, **kwargs)

        for field in self._meta.get_fields():
            if not isinstance(field, LocalizedField):
                continue

            value = getattr(self, field.name, None)

            if not isinstance(value, LocalizedValue):
                if isinstance(value, dict):
                    value = LocalizedValue(value)
                else:
                    value = LocalizedValue()

            setattr(self, field.name, value)
