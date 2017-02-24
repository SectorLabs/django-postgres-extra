from ..fields import HStoreField


class HStoreRequiredSchemaEditorMixin:
    sql_hstore_required_create = (
        'ALTER TABLE {table} '
        'ADD CONSTRAINT {name} '
        'CHECK ({field}->\'{key}\' '
        'IS NOT NULL)'
    )

    sql_hstore_required_rename = (
        'ALTER TABLE {table} '
        'RENAME CONSTRAINT '
        '{old_name} '
        'TO '
        '{new_name}'
    )

    sql_hstore_required_drop = (
        'ALTER TABLE {table} '
        'DROP CONSTRAINT {name}'
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

            for key in self._iterate_required_keys(field):
                self._rename_hstore_required(
                    old_db_table,
                    new_db_table,
                    field,
                    field,
                    key
                )

    def add_field(self, model, field):
        """Ran when a field is added to a model."""

        for key in self._iterate_required_keys(field):
            self._create_hstore_required(
                model._meta.db_table,
                field,
                key
            )

    def remove_field(self, model, field):
        """Ran when a field is removed from a model."""

        for key in self._iterate_required_keys(field):
            self._drop_hstore_required(
                model._meta.db_table,
                field,
                key
            )

    def alter_field(self, model, old_field, new_field, strict=False):
        """Ran when the configuration on a field changed."""

        is_old_field_hstore = isinstance(old_field, HStoreField)
        is_new_field_hstore = isinstance(new_field, HStoreField)

        if not is_old_field_hstore and not is_new_field_hstore:
            return

        old_required = getattr(old_field, 'required', [])
        new_required = getattr(new_field, 'required', [])

        # handle field renames before moving on
        if str(old_field.column) != str(new_field.column):
            for key in self._iterate_required_keys(old_field):
                self._rename_hstore_required(
                    model._meta.db_table,
                    model._meta.db_table,
                    old_field,
                    new_field,
                    key
                )

        # drop the constraints for keys that have been removed
        for key in old_required:
            if key not in new_required:
                self._drop_hstore_required(
                    model._meta.db_table,
                    old_field,
                    key
                )

        # create new constraints for keys that have been added
        for key in new_required:
            if key not in old_required:
                self._create_hstore_required(
                    model._meta.db_table,
                    new_field,
                    key
                )

    def _create_hstore_required(self, table_name, field, key):
        """Creates a REQUIRED CONSTRAINT for the specified hstore key."""

        name = self._required_constraint_name(
            table_name, field, key)

        sql = self.sql_hstore_required_create.format(
            name=self.quote_name(name),
            table=self.quote_name(table_name),
            field=self.quote_name(field.column),
            key=key
        )
        self.execute(sql)

    def _rename_hstore_required(self, old_table_name, new_table_name,
                                old_field, new_field, key):
        """Renames an existing REQUIRED CONSTRAINT for the specified
        hstore key."""

        old_name = self._required_constraint_name(
            old_table_name, old_field, key)
        new_name = self._required_constraint_name(
            new_table_name, new_field, key)

        sql = self.sql_hstore_required_rename.format(
            table=self.quote_name(new_table_name),
            old_name=self.quote_name(old_name),
            new_name=self.quote_name(new_name)
        )
        self.execute(sql)

    def _drop_hstore_required(self, table_name, field, key):
        """Drops a REQUIRED CONSTRAINT for the specified hstore key."""

        name = self._required_constraint_name(
            table_name, field, key)

        sql = self.sql_hstore_required_drop.format(
            table=self.quote_name(table_name),
            name=self.quote_name(name)
        )
        self.execute(sql)

    @staticmethod
    def _required_constraint_name(table: str, field, key):
        """Gets the name for a CONSTRAINT that applies
        to a single hstore key.

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

        Returns:
            The name for the UNIQUE index.
        """

        return '{table}_{field}_required_{postfix}'.format(
            table=table,
            field=field.column,
            postfix=key
        )

    @staticmethod
    def _iterate_required_keys(field):
        """Iterates over the keys marked as "required"
        in the specified field.

        Arguments:
            field:
                The field of which key's to
                iterate over.
        """

        required_keys = getattr(field, 'required', None)
        if not required_keys:
            return

        for key in required_keys:
            yield key
