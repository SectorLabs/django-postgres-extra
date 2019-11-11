from datetime import datetime
from typing import Optional

from dateutil.relativedelta import relativedelta

from .current_time_strategy import PostgresCurrentTimePartitioningStrategy
from .time_partition_size import PostgresTimePartitionSize


class PostgresTimePartitioningStrategy(PostgresCurrentTimePartitioningStrategy):
    def __init__(
        self,
        start_datetime: datetime,
        size: PostgresTimePartitionSize,
        count: int,
        max_age: Optional[relativedelta] = None,
    ) -> None:
        super().__init__(size, count, max_age)

        self.start_datetime = start_datetime

    def get_start_datetime(self) -> datetime:
        return self.start_datetime
