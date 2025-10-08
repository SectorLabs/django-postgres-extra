from typing import List, Type

from django.db.models import Field, Model


def inspect_model_local_concrete_fields(model: Type[Model]) -> List[Field]:
    """Gets a complete list of local and concrete fields on a model, these are
    fields that directly map to a database colmn directly on the table backing
    the model.

    This is similar to Django's `Meta.local_concrete_fields`, which is a
    private API. This method utilizes only public APIs.
    """

    local_concrete_fields = []

    for field in model._meta.get_fields(include_parents=False):
        if isinstance(field, Field) and field.column and not field.many_to_many:
            local_concrete_fields.append(field)

    return local_concrete_fields
