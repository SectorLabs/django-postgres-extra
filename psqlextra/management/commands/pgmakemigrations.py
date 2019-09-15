from django.core.management.commands.makemigrations import \
    Command as MakeMigrationsCommand

from psqlextra.backend.migrations import postgres_migrations


class Command(MakeMigrationsCommand):
    help = "Creates new PostgreSQL specific migration(s) for apps."

    def handle(self, *app_labels, **options):
        with postgres_migrations() as bla:
            return super().handle(*app_labels, **options)
