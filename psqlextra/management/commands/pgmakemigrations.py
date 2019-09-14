from django.core.management.commands.makemigrations import \
    Command as MakeMigrationsCommand

from psqlextra.backend.migrations import patched_autodetector


class Command(MakeMigrationsCommand):
    help = "Creates new PostgreSQL specific migration(s) for apps."

    def handle(self, *app_labels, **options):
        with patched_autodetector() as bla:
            return super().handle(*app_labels, **options)
