from django.db.migrations.operations.models import CreateModel

from psqlextra.backend.migrations.state import PostgresViewModelState


class PostgresCreateViewModel(CreateModel):
    """Creates the model as a native PostgreSQL 11.x view."""

    serialization_expand_args = [
        "fields",
        "options",
        "managers",
        "view_options",
    ]

    def __init__(
        self,
        name,
        fields,
        options=None,
        view_options={},
        bases=None,
        managers=None,
    ):
        super().__init__(name, fields, options, bases, managers)

        self.view_options = view_options or {}

    def state_forwards(self, app_label, state):
        state.add_model(
            PostgresViewModelState(
                app_label=app_label,
                name=self.name,
                fields=list(self.fields),
                options=dict(self.options),
                bases=tuple(self.bases),
                managers=list(self.managers),
                view_options=dict(self.view_options),
            )
        )

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        """Apply this migration operation forwards."""

        model = to_state.apps.get_model(app_label, self.name)
        if self.allow_migrate_model(schema_editor.connection.alias, model):
            schema_editor.create_view_model(model)

    def database_backwards(
        self, app_label, schema_editor, from_state, to_state
    ):
        """Apply this migration operation backwards."""

        model = from_state.apps.get_model(app_label, self.name)
        if self.allow_migrate_model(schema_editor.connection.alias, model):
            schema_editor.delete_view_model(model)

    def deconstruct(self):
        name, args, kwargs = super().deconstruct()

        if self.view_options:
            kwargs["view_options"] = self.view_options

        return name, args, kwargs

    def describe(self):
        """Gets a human readable text describing this migration."""

        description = super().describe()
        description = description.replace("model", "view model")
        return description
