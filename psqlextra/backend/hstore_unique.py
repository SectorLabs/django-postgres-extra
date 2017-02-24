from ..fields import HStoreField


class HStoreUniqueSchemaEditorMixin:
    sql_hstore_unique_create = (
        'CREATE UNIQUE INDEX IF NOT EXISTS '
        '{name} ON {table} '
        '({columns})'
    )

    sql_hstore_unique_rename = (
        'ALTER INDEX '
        '{old_name} '
        'RENAME TO '
        '{new_name}'
    )

    sql_hstore_unique_drop = (
        'DROP INDEX IF EXISTS {name}'
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

    def alter_db_table(self, model, old_db_table, new_db_table):
        """Ran when the name of a model is changed."""

        for field in model._meta.local_fields:
            if not isinstance(field, HStoreField):
                continue

            for keys in self._iterate_uniqueness_keys(field):
                self._rename_hstore_unique(
                    old_db_table,
                    new_db_table,
                    field,
                    field,
                    keys
                )

    def add_field(self, model, field):
        """Ran when a field is added to a model."""

        for keys in self._iterate_uniqueness_keys(field):
            self._create_hstore_unique(
                model,
                field,
                keys
            )

    def remove_field(self, model, field):
        """Ran when a field is removed from a model."""

        for keys in self._iterate_uniqueness_keys(field):
            self._drop_hstore_unique(
                model,
                field,
                keys
            )

    def alter_field(self, model, old_field, new_field, strict=False):
        """Ran when the configuration on a field changed."""

        is_old_field_hstore = isinstance(old_field, HStoreField)
        is_new_field_hstore = isinstance(new_field, HStoreField)

        if not is_old_field_hstore and not is_new_field_hstore:
            return

        old_uniqueness = getattr(old_field, 'uniqueness', [])
        new_uniqueness = getattr(new_field, 'uniqueness', [])

        # handle field renames before moving on
        if str(old_field.column) != str(new_field.column):
            for keys in self._iterate_uniqueness_keys(old_field):
                self._rename_hstore_unique(
                    model._meta.db_table,
                    model._meta.db_table,
                    old_field,
                    new_field,
                    keys
                )

        # drop the indexes for keys that have been removed
        for keys in old_uniqueness:
            if keys not in new_uniqueness:
                self._drop_hstore_unique(
                    model,
                    old_field,
                    self._compose_keys(keys)
                )

        # create new indexes for keys that have been added
        for keys in new_uniqueness:
            if keys not in old_uniqueness:
                self._create_hstore_unique(
                    model,
                    new_field,
                    self._compose_keys(keys)
                )

    def _create_hstore_unique(self, model, field, keys):
        """Creates a UNIQUE constraint for the specified hstore keys."""

        name = self._unique_constraint_name(
            model._meta.db_table, field, keys)
        columns = [
            '(%s->\'%s\')' % (field.column, key)
            for key in keys
        ]
        sql = self.sql_hstore_unique_create.format(
            name=self.quote_name(name),
            table=self.quote_name(model._meta.db_table),
            columns=','.join(columns)
        )
        self.execute(sql)

    def _rename_hstore_unique(self, old_table_name, new_table_name,
                              old_field, new_field, keys):
        """Renames an existing UNIQUE constraint for the specified
        hstore keys."""

        old_name = self._unique_constraint_name(
            old_table_name, old_field, keys)
        new_name = self._unique_constraint_name(
            new_table_name, new_field, keys)

        sql = self.sql_hstore_unique_rename.format(
            old_name=self.quote_name(old_name),
            new_name=self.quote_name(new_name)
        )
        self.execute(sql)

    def _drop_hstore_unique(self, model, field, keys):
        """Drops a UNIQUE constraint for the specified hstore keys."""

        name = self._unique_constraint_name(
            model._meta.db_table, field, keys)
        sql = self.sql_hstore_unique_drop.format(name=self.quote_name(name))
        self.execute(sql)

    @staticmethod
    def _unique_constraint_name(table: str, field, keys):
        """Gets the name for a UNIQUE INDEX that applies
        to one or more keys in a hstore field.

        Arguments:
            table:
                The name of the table the field is
                a part of.

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
            table=table,
            field=field.column,
            postfix=postfix
        )

    def _iterate_uniqueness_keys(self, field):
        """Iterates over the keys marked as "unique"
        in the specified field.

        Arguments:
            field:
                The field of which key's to
                iterate over.
        """

        uniqueness = getattr(field, 'uniqueness', None)
        if not uniqueness:
            return

        for keys in uniqueness:
            composed_keys = self._compose_keys(keys)
            yield composed_keys

    @staticmethod
    def _compose_keys(constraint):
        """Turns a string into a list of string or
        returns it as a list."""
        if isinstance(constraint, str):
            return [constraint]

        return constraint
