from django.db import migrations
from django.db.migrations.state import ProjectState
from django.db.migrations.autodetector import MigrationAutodetector

from psqlextra import HStoreField


def make_project_state(model_states):
    """Shortcut to make :see:ProjectState from a list
    of predefined models."""

    project_state = ProjectState()
    for model_state in model_states:
        project_state.add_model(model_state.clone())
    return project_state


def detect_changes(before_states, after_states):
    """Uses the migration autodetector to detect changes
    in the specified project states."""

    return MigrationAutodetector(
        make_project_state(before_states),
        make_project_state(after_states)
    )._detect_changes()


def assert_autodetector(changes, expected):
    """Asserts whether the results of the auto detector
    are as expected."""

    assert 'tests' in changes
    assert len('tests') > 0

    operations = changes['tests'][0].operations

    for i, expected_operation in enumerate(expected):
        real_operation = operations[i]
        _, _, real_args, real_kwargs = real_operation.field.deconstruct()
        _, _, expected_args, expected_kwargs = expected_operation.field.deconstruct()

        assert real_args == expected_args
        assert real_kwargs == expected_kwargs


def test_uniqueness():
    """Tests whether changes in the `uniqueness`
    option are properly detected by the auto detector."""

    before = [
        migrations.state.ModelState('tests', 'Model1', [
            ('title', HStoreField())
        ])
    ]
    after = [
        migrations.state.ModelState('tests', 'Model1', [
            ('title', HStoreField(uniqueness=['en']))
        ])
    ]

    changes = detect_changes(before, after)

    assert_autodetector(changes, [
        migrations.AlterField(
            'Model1',
            'title',
            HStoreField(uniqueness=['en'])
        )
    ])


def test_required():
    """Tests whether changes in the `required`
    option are properly detected by the auto detector."""

    before = [
        migrations.state.ModelState('tests', 'Model1', [
            ('title', HStoreField())
        ])
    ]
    after = [
        migrations.state.ModelState('tests', 'Model1', [
            ('title', HStoreField(required=['en']))
        ])
    ]

    changes = detect_changes(before, after)

    assert_autodetector(changes, [
        migrations.AlterField(
            'Model1',
            'title',
            HStoreField(required=['en'])
        )
    ])
