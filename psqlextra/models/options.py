from typing import Dict, List, Optional, Union

from django.db.models import BaseConstraint

from psqlextra.types import PostgresPartitioningMethod, SQLWithParams


class PostgresPartitionedModelOptions:
    """Container for :see:PostgresPartitionedModel options.

    This is where attributes copied from the model's `PartitioningMeta`
    are held.
    """

    def __init__(
            self, method: PostgresPartitioningMethod, key: List[str], per_partition_constraints: list[BaseConstraint]
    ):
        self.method = method
        self.key = key
        self.original_attrs: Dict[
            str, Union[PostgresPartitioningMethod, List[str]]
        ] = dict(method=method, key=key)
        self.per_partition_constraints = per_partition_constraints or []


class PostgresViewOptions:
    """Container for :see:PostgresView and :see:PostgresMaterializedView
    options.

    This is where attributes copied from the model's `ViewMeta` are
    held.
    """

    def __init__(self, query: Optional[SQLWithParams]):
        self.query = query
        self.original_attrs: Dict[str, Optional[SQLWithParams]] = dict(
            query=self.query
        )
