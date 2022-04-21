from importlib import import_module

from psqlextra.compiler import (
    SQLAggregateCompiler,
    SQLCompiler,
    SQLDeleteCompiler,
    SQLInsertCompiler,
    SQLUpdateCompiler,
)

from . import base_impl


class PostgresOperations(base_impl.operations()):
    """Simple operations specific to PostgreSQL."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._compiler_cache = None

    def compiler(self, compiler_name: str):
        """Gets the SQL compiler with the specified name."""

        if self._cache is None:
            self._cache = import_module('psqlextra.compiler')

        # Let any parent module try to find the compiler as fallback. Better run without caller comment than break
        return getattr(self._cache, compiler_name, super().compiler(compiler_name))
