from ..fields import HStoreField


class HStoreRequiredSchemaEditorMixin:
    sql_hstore_required_create = (
        'ALTER TABLE "{table}" '
        'ADD CONSTRAINT "{name}" '
        'CHECK ({field}->\'{key}\' '
        'IS NOT NULL)'
    )

    sql_hstore_required_drop = (
        'ALTER TABLE "{table}" '
        'DROP CONSTRAINT "{name}"'
    )

    @staticmethod
    def _required_constraint_name(model, field, key: str):
        """Gets the name to give to the constraint
        that applies to a single key in a hstore field.

        Argument:
            model:
                The model the field is a part of.

            field:
                The hstore field to create the
                constraint for.

            key:
                The name of the key to create
                the constraint for.

        Returns:
            The name for the constraint.
        """

        return '{table}_{field}_notnull_{key}'.format(
            table=model._meta.db_table,
            field=field.column,
            key=key
        )

    def _create_hstore_required(self, model, field, key):
        """Creates a NOT NULL constraint for the specified
        field and key."""

        name = self._required_constraint_name(model, field, key)
        sql = self.sql_hstore_required_create.format(
            name=name,
            table=model._meta.db_table,
            key=key
        )
        self.execute(sql)

    def _drop_hstore_required(self, model, field, key):
        """Drops the NOT NULL constraint for the specified
        field and key."""

        name = self._required_constraint_name(model, field, key)
        sql = self.sql_hstore_required_drop.format(
            name=name,
            table=model._meta.db_table,
            key=key
        )
        self.execute(sql)

    def _update_hstore_required(self, model, old_field, new_field):
        """Updates the NOT NULL constraints for the specified field."""

        old_required = getattr(old_field, 'required', []) or []
        new_required = getattr(new_field, 'required', []) or []

        for key in old_required:
            if key not in new_required:
                self._drop_hstore_required(
                    model,
                    old_field,
                    key
                )

        for key in new_required:
            if key not in old_required:
                self._create_hstore_required(
                    model,
                    new_field,
                    key
                )

    def alter_field(self, model, old_field, new_field, *args, **kwargs):
        """Ran when the configuration on a field changed."""

        is_old_field_hstore = isinstance(old_field, HStoreField)
        is_new_field_hstore = isinstance(new_field, HStoreField)

        if is_old_field_hstore or is_new_field_hstore:
            self._update_hstore_required(model, old_field, new_field)

    def add_field(self, model, field):
        """Ran when a field is added to a model."""

    def remove_field(self, model, field):
        """ran when a field is removed from a model."""

    def create_model(self, model):
        """Ran when a new model is created."""

        for field in model._meta.local_fields:
            if not isinstance(field, HStoreField):
                continue

    def delete_model(self, model):
        """Ran when a model is being deleted."""

        for field in model._meta.local_fields:
            if not isinstance(field, HStoreField):
                continue
