from django.db import models
from django.db.models.expressions import CombinedExpression, Func


class Equals(CombinedExpression):
    """Expression that constructs `{lhs} = {rhs}`.

    Used as an alternative to Django's `Q` object when the
    left-hand side is a aliased field not known to Django.
    """

    connector: str = "="

    def __init__(self, lhs, rhs) -> None:
        super().__init__(
            lhs, self.connector, rhs, output_field=models.BooleanField()
        )


class Is(Equals):
    """Expression that constructs `{lhs} IS {rhs}`."""

    connector: str = "IS"


class GreaterThen(Equals):
    """Expression that constructs `{lhs} > {rhs}`."""

    connector: str = ">"


class LowerThenOrEqual(Equals):
    """Expression that constructs `{lhs} <= {rhs}`."""

    connector: str = "<="


class And(Equals):
    """Expression that constructs `{lhs} AND {rhs}`."""

    connector: str = "AND"


class Bool(Func):
    """Cast to a boolean."""

    function = "BOOL"
