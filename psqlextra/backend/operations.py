from importlib import import_module

from . import base_impl


class PostgresOperations(base_impl.operations()):
    """Simple operations specific to PostgreSQL."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._compiler_cache = None

    def compiler(self, compiler_name: str):
        """Gets the SQL compiler with the specified name."""

        # first let django try to find the compiler
        try:
            return super().compiler(compiler_name)
        except AttributeError:
            pass

        # django can't find it, look in our own module
        if self._compiler_cache is None:
            self._compiler_cache = import_module("psqlextra.compiler")

        return getattr(self._compiler_cache, compiler_name)
