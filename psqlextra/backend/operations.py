from psqlextra.compiler import (
    PostgresAggregateCompiler,
    PostgresCompiler,
    PostgresDeleteCompiler,
    PostgresInsertCompiler,
    PostgresUpdateCompiler,
)

from . import base_impl


class PostgresOperations(base_impl.operations()):
    """Simple operations specific to PostgreSQL."""

    compiler_map = {
        "SQLCompiler": PostgresCompiler,
        "SQLInsertCompiler": PostgresInsertCompiler,
        "SQLUpdateCompiler": PostgresUpdateCompiler,
        "SQLDeleteCompiler": PostgresDeleteCompiler,
        "SQLAggregateCompiler": PostgresAggregateCompiler,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._compiler_cache = None

    def compiler(self, compiler_name: str):
        """Gets the SQL compiler with the specified name."""

        postgres_compiler = self.compiler_map.get(compiler_name)
        if postgres_compiler:
            return postgres_compiler

        # Let Django try to find the compiler. Better run without caller comment than break
        return super().compiler(compiler_name)
