def setup():
    """Initializes the django-postgres-extra library.

    This mainly adds custom meta options names to Django's
    django.db.models.options.DEFAULT_NAMES so that they
    get included in migrations."""

    import django.db.models.options as options
    new_meta_options = (
        'view_query',
    )

    options.DEFAULT_NAMES = options.DEFAULT_NAMES + new_meta_options

# this has to be here to make sure that setup() is called
# before django initializes, bit of a hack, but hey it works
setup()
