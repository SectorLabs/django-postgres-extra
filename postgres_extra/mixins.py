from django.db import transaction
from django.conf import settings
from django.db.utils import IntegrityError


class AtomicSlugRetryMixin:
    """Makes :see:LocalizedUniqueSlugField work by retrying upon
    violation of the UNIQUE constraint."""

    def save(self, *args, **kwargs):
        """Saves this model instance to the database."""

        max_retries = getattr(
            settings,
            'LOCALIZED_FIELDS_MAX_RETRIES',
            100
        )

        if not hasattr(self, 'retries'):
            self.retries = 0

        with transaction.atomic():
            try:
                return super().save(*args, **kwargs)
            except IntegrityError as ex:
                # this is as retarded as it looks, there's no
                # way we can put the retry logic inside the slug
                # field class... we can also not only catch exceptions
                # that apply to slug fields... so yea.. this is as
                # retarded as it gets... i am sorry :(
                if 'slug' not in str(ex):
                    raise ex

                if self.retries >= max_retries:
                    raise ex

        self.retries += 1
        return self.save()
