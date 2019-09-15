from django.core.management.commands import makemigrations

from psqlextra.backend.migrations import postgres_patched_migrations


class Command(makemigrations.Command):
    help = "Creates new PostgreSQL specific migration(s) for apps."

    def handle(self, *app_labels, **options):
        with postgres_patched_migrations():
            return super().handle(*app_labels, **options)
