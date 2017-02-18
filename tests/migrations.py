from typing import List
from unittest import mock
from contextlib import contextmanager
import copy

from django.db import connection
from django.apps import apps
from django.db.backends.base.schema import BaseDatabaseSchemaEditor

from .fake_model import define_fake_model


@contextmanager
def migration_test(*filters: List[str]):
    """Assists in testing the custom back-end
    during a migration.

    Arguments:
        filters:
            List of strings to filter SQL
            statements on.
    """

    with mock.patch.object(BaseDatabaseSchemaEditor, 'execute') as execute:
        filter_results = {}
        with connection.schema_editor() as schema_editor:
            yield schema_editor, filter_results

    for filter_text in filters:
        filter_results[filter_text] = [
            call for call in execute.mock_calls
            if filter_text in str(call)
        ]


@contextmanager
def create_drop_model(field, filters: List[str]):
    """Creates and drops a model with the specified field.

    Arguments:
        field:
            The field to include on the
            model to create and drop.

        filters:
            List of strings to filter
            SQL statements on.
    """

    model = define_fake_model({'title': field})

    with migration_test(*filters) as (schema_editor, calls):
        schema_editor.create_model(model)
        schema_editor.delete_model(model)

    yield calls


@contextmanager
def add_field(field, filters: List[str]):
    """Adds the specified field to a model.

    Arguments:
        field:
            The field to add to a model.

        filters:
            List of strings to filter
            SQL statements on.
    """

    model = define_fake_model({'title': field})
    app_config = apps.get_app_config('tests')

    with migration_test(*filters) as (schema_editor, calls):
        dynmodel = app_config.get_model(model.__name__)
        schema_editor.add_field(dynmodel, dynmodel._meta.fields[1])

    yield calls


@contextmanager
def remove_field(field, filters: List[str]):
    """Removes the specified field from a model.

    Arguments:
        field:
            The field to remove from a model.

        filters:
            List of strings to filter
            SQL statements on.
    """

    model = define_fake_model({'title': field})
    app_config = apps.get_app_config('tests')

    with migration_test(*filters) as (schema_editor, calls):
        dynmodel = app_config.get_model(model.__name__)
        schema_editor.remove_field(dynmodel, dynmodel._meta.fields[1])

    yield calls


@contextmanager
def alter_field(old_field, new_field, filters: List[str]):
    """Alters a field from one state to the other.

    Arguments:
        old_field:
            The field before altering it.

        new_field:
            The field after altering it.

        filters:
            List of strings to filter
            SQL statements on.
    """

    model = define_fake_model({'title': old_field})
    app_config = apps.get_app_config('tests')

    with migration_test(*filters) as (schema_editor, calls):
        dynmodel = app_config.get_model(model.__name__)

        # this is nasty, we cannot pass on `new_field`
        # because it wasn't processed by django, we simply
        # have to copy the modified kwargs instead
        altered_field = copy.deepcopy(dynmodel._meta.fields[1])
        _, _, _, kwargs = new_field.deconstruct()
        for kwname, kwvalue in kwargs.items():
            setattr(altered_field, kwname, kwvalue)

        schema_editor.alter_field(
            dynmodel,
            dynmodel._meta.fields[1],
            altered_field
        )

    yield calls
