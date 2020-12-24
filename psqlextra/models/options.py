from typing import Dict, List, Optional, Union

from psqlextra.types import SQL, PostgresPartitioningMethod, SQLWithParams


class PostgresPartitionedModelOptions:
    """Container for :see:PostgresPartitionedModel options.

    This is where attributes copied from the model's `PartitioningMeta`
    are held.
    """

    def __init__(self, method: PostgresPartitioningMethod, key: List[str]):
        self.method = method
        self.key = key
        self.original_attrs: Dict[
            str, Union[PostgresPartitioningMethod, List[key]]
        ] = dict(method=method, key=key)


class PostgresViewOptions:
    """Container for :see:PostgresView and :see:PostgresMaterializedView
    options.

    This is where attributes copied from the model's `ViewMeta` are
    held.
    """

    def __init__(
        self, query: Optional[SQLWithParams], unique_constraint: Optional[SQL]
    ):
        self.query = query
        self.unique_constraint = unique_constraint
        self.original_attrs: Dict[
            str, Union[Optional[SQLWithParams], Optional[SQL]]
        ] = dict(query=self.query, unique_constraint=self.unique_constraint)
