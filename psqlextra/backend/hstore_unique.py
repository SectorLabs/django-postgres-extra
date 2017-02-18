from ..fields import HStoreField


class HStoreUniqueSchemaEditorMixin:
    sql_hstore_unique_create = (
        'CREATE UNIQUE INDEX IF NOT EXISTS '
        '"{name}" ON "{table}" '
        '({columns})'
    )

    sql_hstore_unique_drop = (
        'DROP INDEX IF EXISTS "{name}"'
    )

    @staticmethod
    def _unique_constraint_name(model, field, keys):
        """Gets the name for a UNIQUE INDEX that applies
        to one or more keys in a hstore field.

        Arguments:
            model:
                The model the field is a part of.

            field:
                The hstore field to create a
                UNIQUE INDEX for.

            key:
                The name of the hstore key
                to create the name for.

                This can also be a tuple
                of multiple names.

        Returns:
            The name for the UNIQUE index.
        """
        postfix = '_'.join(keys)
        return '{table}_{field}_unique_{postfix}'.format(
            table=model._meta.db_table,
            field=field.column,
            postfix=postfix
        )

    def _drop_hstore_unique(self, model, field, keys):
        """Drops a UNIQUE constraint for the specified hstore keys."""

        name = self._unique_constraint_name(model, field, keys)
        sql = self.sql_hstore_unique_drop.format(name=name)
        self.execute(sql)

    def _create_hstore_unique(self, model, field, keys):
        """Creates a UNIQUE constraint for the specified hstore keys."""

        name = self._unique_constraint_name(model, field, keys)
        columns = [
            '(%s->\'%s\')' % (field.column, key)
            for key in keys
        ]
        sql = self.sql_hstore_unique_create.format(
            name=name,
            table=model._meta.db_table,
            columns=','.join(columns)
        )
        self.execute(sql)

    def _apply_hstore_uniqueness(self, method, model, field):
        """Creates/drops UNIQUE constraints for a field."""

        uniqueness = getattr(field, 'uniqueness', None)
        if not uniqueness:
            return

        for keys in uniqueness:
            method(
                model,
                field,
                self._compose_keys(keys)
            )

    @staticmethod
    def _compose_keys(constraint):
        if isinstance(constraint, str):
            return [constraint]

        return constraint

    def _update_hstore_constraints(self, model, old_field, new_field):
        """Updates the UNIQUE constraints for the specified field."""

        old_uniqueness = getattr(old_field, 'uniqueness', None)
        new_uniqueness = getattr(new_field, 'uniqueness', None)

        for keys in old_uniqueness:
            if keys not in new_uniqueness:
                self._drop_hstore_unique(
                    model,
                    old_field,
                    self._compose_keys(keys)
                )

        for keys in new_uniqueness:
            if keys not in old_uniqueness:
                self._create_hstore_unique(
                    model,
                    new_field,
                    self._compose_keys(keys)
                )

    def alter_field(self, model, old_field, new_field, *args, **kwargs):
        """Ran when the configuration on a field changed."""

        is_old_field_hstore = isinstance(old_field, HStoreField)
        is_new_field_hstore = isinstance(new_field, HStoreField)

        if is_old_field_hstore or is_new_field_hstore:
            self._update_hstore_constraints(model, old_field, new_field)

    def add_field(self, model, field):
        """Ran when a field is added to a model."""

        self._apply_hstore_uniqueness(
            self._create_hstore_unique,
            model,
            field
        )

    def remove_field(self, model, field):
        """ran when a field is removed from a model."""

        self._apply_hstore_uniqueness(
            self._drop_hstore_unique,
            model,
            field
        )

    def create_model(self, model):
        """Ran when a new model is created."""

        for field in model._meta.local_fields:
            if not isinstance(field, HStoreField):
                continue

            self.add_field(model, field)

    def delete_model(self, model):
        """Ran when a model is being deleted."""

        for field in model._meta.local_fields:
            if not isinstance(field, HStoreField):
                continue

            self.remove_field(model, field)
