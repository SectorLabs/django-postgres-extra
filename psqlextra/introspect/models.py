from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
)

from django.core.exceptions import FieldDoesNotExist
from django.db import connection, models
from django.db.models import Field, Model
from django.db.models.expressions import Expression

from .fields import inspect_model_local_concrete_fields

TModel = TypeVar("TModel", bound=models.Model)


def _construct_model(
    model: Type[TModel],
    columns: Iterable[str],
    values: Iterable[Any],
    *,
    apply_converters: bool = True
) -> TModel:
    fields_by_name_and_column = {}
    for concrete_field in inspect_model_local_concrete_fields(model):
        fields_by_name_and_column[concrete_field.attname] = concrete_field

        if concrete_field.db_column:
            fields_by_name_and_column[concrete_field.db_column] = concrete_field

    indexable_columns = list(columns)

    row = {}

    for index, value in enumerate(values):
        column = indexable_columns[index]
        try:
            field: Optional[Field] = cast(Field, model._meta.get_field(column))
        except FieldDoesNotExist:
            field = fields_by_name_and_column.get(column)

        if not field:
            continue

        field_column_expression = field.get_col(model._meta.db_table)

        if apply_converters:
            converters = cast(Expression, field).get_db_converters(
                connection
            ) + connection.ops.get_db_converters(field_column_expression)

            converted_value = value
            for converter in converters:
                converted_value = converter(
                    converted_value,
                    field_column_expression,
                    connection,
                )
        else:
            converted_value = value

        row[field.attname] = converted_value

    instance = model(**row)
    instance._state.adding = False
    instance._state.db = connection.alias

    return instance


def models_from_cursor(
    model: Type[TModel], cursor, *, related_fields: List[str] = []
) -> Generator[TModel, None, None]:
    """Fetches all rows from a cursor and converts the values into model
    instances.

    This is roughly what Django does internally when you do queries. This
    goes further than `Model.from_db` as it also applies converters to make
    sure that values are converted into their Python equivalent.

    Use this when you've outgrown the ORM and you are writing performant
    queries yourself and you need to map the results back into ORM objects.

    Arguments:
        model:
            Model to construct.

        cursor:
            Cursor to read the rows from.

        related_fields:
            List of ForeignKey/OneToOneField names that were joined
            into the raw query. Use this to achieve the same thing
            that Django's `.select_related()` does.

            Field names should be specified in the order that they
            are SELECT'd in.
    """

    columns = [col[0] for col in cursor.description]
    field_offset = len(inspect_model_local_concrete_fields(model))

    rows = cursor.fetchmany()

    while rows:
        for values in rows:
            instance = _construct_model(
                model, columns[:field_offset], values[:field_offset]
            )

            for index, related_field_name in enumerate(related_fields):
                related_model = cast(
                    Union[Type[Model], None],
                    model._meta.get_field(related_field_name).related_model,
                )
                if not related_model:
                    continue

                related_field_count = len(
                    inspect_model_local_concrete_fields(related_model)
                )

                # autopep8: off
                related_columns = columns[
                    field_offset : field_offset + related_field_count  # noqa
                ]
                related_values = values[
                    field_offset : field_offset + related_field_count  # noqa
                ]
                # autopep8: one

                if (
                    not related_columns
                    or not related_values
                    or all([value is None for value in related_values])
                ):
                    continue

                related_instance = _construct_model(
                    cast(Type[Model], related_model),
                    related_columns,
                    related_values,
                )
                instance._state.fields_cache[related_field_name] = related_instance  # type: ignore

                field_offset += len(
                    inspect_model_local_concrete_fields(related_model)
                )

            yield instance

        rows = cursor.fetchmany()


def model_from_cursor(
    model: Type[TModel], cursor, *, related_fields: List[str] = []
) -> Optional[TModel]:
    return next(
        models_from_cursor(model, cursor, related_fields=related_fields), None
    )


def model_from_dict(
    model: Type[TModel], row: Dict[str, Any], *, apply_converters: bool = True
) -> TModel:
    return _construct_model(
        model, row.keys(), row.values(), apply_converters=apply_converters
    )
