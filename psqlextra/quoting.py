import re

# One or more well formed quoted identifiers, such as "table" or "schema"."table"
_QUOTED_IDENTIFIER = re.compile(r'"(?:[^"]|"")*"(?:\."(?:[^"]|"")*")*')


def quote_identifier(name: str) -> str:
    """Quotes a name for use as an identifier in SQL.

    Django's ``quote_name`` does not escape double quotes inside the
    name, so a name containing one can end the identifier early and
    inject SQL. Like ``quote_name``, this returns a name that is already
    quoted unchanged.
    """

    if _QUOTED_IDENTIFIER.fullmatch(name):
        return name

    return '"%s"' % name.replace('"', '""')
