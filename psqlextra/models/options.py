from typing import Dict, List, Optional, Union

from psqlextra.types import PostgresPartitioningMethod, SQLWithParams


class PostgresPartitionedModelOptions:
    """Container for :see:PostgresPartitionedModel options.

    This is where attributes copied from the model's `PartitioningMeta`
    are held.
    """

    def __init__(
        self,
        method: PostgresPartitioningMethod,
        key: List[str],
        sub_key: Optional[List[str]] = None,
        sub_method: Optional[PostgresPartitioningMethod] = None,
    ):
        self.method = method
        self.key = key
        self.sub_method = sub_method if sub_method else None
        self.sub_key = sub_key if sub_key else []
        self.original_attrs: Dict[
            str,
            Union[
                PostgresPartitioningMethod,
                List[str],
                PostgresPartitioningMethod,
                List[str],
                None,
            ],
        ] = dict(
            method=method,
            key=key,
            sub_method=sub_method,
            sub_key=sub_key,
        )


class PostgresViewOptions:
    """Container for :see:PostgresView and :see:PostgresMaterializedView
    options.

    This is where attributes copied from the model's `ViewMeta` are
    held.
    """

    def __init__(self, query: Optional[SQLWithParams]):
        self.query = query
        self.original_attrs: Dict[str, Optional[SQLWithParams]] = dict(query=self.query)
