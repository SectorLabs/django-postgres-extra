import enum

from datetime import date, datetime
from typing import Optional, Union

from dateutil.relativedelta import relativedelta

from .error import PostgresPartitioningError


class PostgresTimePartitionUnit(enum.Enum):
    YEARS = "years"
    MONTHS = "months"
    WEEKS = "weeks"
    DAYS = "days"


class PostgresTimePartitionSize:
    """Size of a time-based range partition table."""

    unit: PostgresTimePartitionUnit
    value: int

    def __init__(
        self,
        years: Optional[int] = None,
        months: Optional[int] = None,
        weeks: Optional[int] = None,
        days: Optional[int] = None,
    ) -> None:
        sizes = [years, months, weeks, days]

        if not any(sizes):
            raise PostgresPartitioningError("Partition cannot be 0 in size.")

        if len([size for size in sizes if size and size > 0]) > 1:
            raise PostgresPartitioningError(
                "Partition can only have on size unit."
            )

        if years:
            self.unit = PostgresTimePartitionUnit.YEARS
            self.value = years
        elif months:
            self.unit = PostgresTimePartitionUnit.MONTHS
            self.value = months
        elif weeks:
            self.unit = PostgresTimePartitionUnit.WEEKS
            self.value = weeks
        elif days:
            self.unit = PostgresTimePartitionUnit.DAYS
            self.value = days
        else:
            raise PostgresPartitioningError(
                "Unsupported time partitioning unit"
            )

    def as_delta(self) -> relativedelta:
        if self.unit == PostgresTimePartitionUnit.YEARS:
            return relativedelta(years=self.value)

        if self.unit == PostgresTimePartitionUnit.MONTHS:
            return relativedelta(months=self.value)

        if self.unit == PostgresTimePartitionUnit.WEEKS:
            return relativedelta(weeks=self.value)

        if self.unit == PostgresTimePartitionUnit.DAYS:
            return relativedelta(days=self.value)

        raise PostgresPartitioningError(
            "Unsupported time partitioning unit: %s" % self.unit
        )

    def start(self, dt: datetime) -> datetime:
        if self.unit == PostgresTimePartitionUnit.YEARS:
            return self._ensure_datetime(dt.replace(month=1, day=1))

        if self.unit == PostgresTimePartitionUnit.MONTHS:
            return self._ensure_datetime(dt.replace(day=1))

        if self.unit == PostgresTimePartitionUnit.WEEKS:
            return self._ensure_datetime(dt - relativedelta(days=dt.weekday()))

        return self._ensure_datetime(dt)

    @staticmethod
    def _ensure_datetime(dt: Union[date, datetime]) -> datetime:
        return datetime(year=dt.year, month=dt.month, day=dt.day)

    def __repr__(self) -> str:
        return "PostgresTimePartitionSize<%s, %s>" % (self.unit, self.value)


__all__ = ["PostgresTimePartitionUnit", "PostgresTimePartitionSize"]
