from ..fields import HStoreField


class HStoreUniqueSchemaEditorMixin:
    sql_hstore_unique_create = "CREATE UNIQUE INDEX {name} ON {table}{using} ({columns}){extra}"
    sql_hstore_unique_drop = "DROP INDEX IF EXISTS {name}"

    @staticmethod
    def _hstore_unique_name(model, field, keys):
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
        return '{table_name}_{field_name}_unique_{postfix}'.format(
            table_name=model._meta.db_table,
            field_name=field.column,
            postfix=postfix
        )

    def _drop_hstore_unique(self, model, field, keys):
        """Drops a UNIQUE constraint for the specified hstore keys."""

        name = self._hstore_unique_name(model, field, keys)
        sql = self.sql_hstore_unique_drop.format(name=name)
        self.execute(sql)

    def _create_hstore_unique(self, model, field, keys):
        """Creates a UNIQUE constraint for the specified hstore keys."""

        name = self._hstore_unique_name(model, field, keys)
        columns = [
            '(%s->\'%s\')' % (field.column, key)
            for key in keys
        ]
        sql = self.sql_hstore_unique_create.format(
            name=name,
            table=model._meta.db_table,
            using='',
            columns=','.join(columns),
            extra=''
        )
        self.execute(sql)

    def _apply_hstore_constraints(self, method, model, field):
        """Creates/drops UNIQUE constraints for a field."""

        def _compose_keys(constraint):
            if isinstance(constraint, str):
                return [constraint]

            return constraint

        uniqueness = getattr(field, 'uniqueness', None)
        if not uniqueness:
            return

        for keys in uniqueness:
            method(
                model,
                field,
                _compose_keys(keys)
            )

    def _update_hstore_constraints(self, model, old_field, new_field):
        """Updates the UNIQUE constraints for the specified field."""

        old_uniqueness = getattr(old_field, 'uniqueness', None)
        new_uniqueness = getattr(new_field, 'uniqueness', None)

        # drop any old uniqueness constraints
        if old_uniqueness:
            self._apply_hstore_constraints(
                self._drop_hstore_unique,
                model,
                old_field
            )

        # (re-)create uniqueness constraints
        if new_uniqueness:
            self._apply_hstore_constraints(
                self._create_hstore_unique,
                model,
                new_field
            )

    def _alter_field(self, model, old_field, new_field, *args, **kwargs):
        """Ran when the configuration on a field changed."""

        super()._alter_field(
            model, old_field, new_field,
            *args, **kwargs
        )

        is_old_field_localized = isinstance(old_field, HStoreField)
        is_new_field_localized = isinstance(new_field, HStoreField)

        if is_old_field_localized or is_new_field_localized:
            self._update_hstore_constraints(model, old_field, new_field)

    def create_model(self, model):
        """Ran when a new model is created."""

        super().create_model(model)

        for field in model._meta.local_fields:
            if not isinstance(field, HStoreField):
                continue

            self._apply_hstore_constraints(
                self._create_hstore_unique,
                model,
                field
            )

    def delete_model(self, model):
        """Ran when a model is being deleted."""

        super().delete_model(model)

        for field in model._meta.local_fields:
            if not isinstance(field, HStoreField):
                continue

            self._apply_hstore_constraints(
                self._drop_hstore_unique,
                model,
                field
            )
