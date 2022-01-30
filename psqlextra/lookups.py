from django.db.models import lookups
from django.db.models.fields import Field, related_lookups
from django.db.models.fields.related import ForeignObject


class InValuesLookupMixin:
    """Performs a `lhs IN VALUES ((a), (b), (c))` lookup.

    This can be significantly faster then a normal `IN (a, b, c)`. The
    latter sometimes causes the Postgres query planner do a sequential
    scan.
    """

    def as_sql(self, compiler, connection):

        if not self.rhs_is_direct_value():
            return super().as_sql(compiler, connection)

        lhs, lhs_params = self.process_lhs(compiler, connection)

        _, rhs_params = self.process_rhs(compiler, connection)
        rhs = ",".join([f"(%s)" for _ in rhs_params])  # noqa: F541

        return f"{lhs} IN (VALUES {rhs})", lhs_params + list(rhs_params)


@Field.register_lookup
class InValuesLookup(InValuesLookupMixin, lookups.In):
    lookup_name = "invalues"


@ForeignObject.register_lookup
class InValuesRelatedLookup(InValuesLookupMixin, related_lookups.RelatedIn):
    lookup_name = "invalues"
