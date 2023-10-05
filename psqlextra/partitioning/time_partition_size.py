import enum

from datetime import date, datetime, timedelta, timezone
from typing import Optional, Union

from dateutil.relativedelta import relativedelta

from .error import PostgresPartitioningError


class PostgresTimePartitionUnit(enum.Enum):
    YEARS = "years"
    MONTHS = "months"
    WEEKS = "weeks"
    DAYS = "days"
    HOURS = "hours"


UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class PostgresTimePartitionSize:
    """Size of a time-based range partition table."""

    unit: PostgresTimePartitionUnit
    value: int
    anchor: datetime

    def __init__(
        self,
        years: Optional[int] = None,
        months: Optional[int] = None,
        weeks: Optional[int] = None,
        days: Optional[int] = None,
        hours: Optional[int] = None,
        anchor: datetime = UNIX_EPOCH,
    ) -> None:
        sizes = [years, months, weeks, days, hours]

        if not any(sizes):
            raise PostgresPartitioningError("Partition cannot be 0 in size.")

        if len([size for size in sizes if size and size > 0]) > 1:
            raise PostgresPartitioningError(
                "Partition can only have on size unit."
            )

        self.anchor = anchor
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
        elif hours:
            self.unit = PostgresTimePartitionUnit.HOURS
            self.value = hours
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

        if self.unit == PostgresTimePartitionUnit.HOURS:
            return relativedelta(hours=self.value)

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

        if self.unit == PostgresTimePartitionUnit.DAYS:
            diff_days = (dt - self.anchor).days
            partition_index = diff_days // self.value
            start = self.anchor + timedelta(days=partition_index * self.value)
            return self._ensure_datetime(start)

        if self.unit == PostgresTimePartitionUnit.HOURS:
            return self._ensure_datetime(dt.replace(hour=0))

        raise ValueError("Unknown unit")

    @staticmethod
    def _ensure_datetime(dt: Union[date, datetime]) -> datetime:
        hour = dt.hour if isinstance(dt, datetime) else 0
        return datetime(year=dt.year, month=dt.month, day=dt.day, hour=hour)

    def __repr__(self) -> str:
        return "PostgresTimePartitionSize<%s, %s>" % (self.unit, self.value)


__all__ = ["PostgresTimePartitionUnit", "PostgresTimePartitionSize"]
