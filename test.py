#!/usr/bin/env python
import os
import sys

os.environ.setdefault(
    'DJANGO_SETTINGS_MODULE',
    'settings'
)


import django
django.setup()

from psqlextra.models import PostgresPartitionedModel

print(PostgresPartitionedModel._partitioning_meta)
