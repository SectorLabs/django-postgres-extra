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

    compiler_module = "psqlextra.compiler"

    compiler_classes = [
        SQLCompiler,
        SQLDeleteCompiler,
        SQLAggregateCompiler,
        SQLUpdateCompiler,
        SQLInsertCompiler,
    ]

    def default_schema_name(self) -> str:
        return "public"
