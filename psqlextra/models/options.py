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
        submethod: PostgresPartitioningMethod | None = None,
        subkey: List[str] | None = None,
    ):
        self.method = method
        self.key = key
        self.submethod = submethod
        self.subkey = subkey
        self.original_attrs: Dict[str, Union[None, PostgresPartitioningMethod, List[str]]] = dict(
            method=method, key=key, submethod=submethod, subkey=subkey
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
