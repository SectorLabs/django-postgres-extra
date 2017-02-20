from enum import Enum


class ConflictAction(Enum):
    """Possible actions to take when a conflict arises."""

    Nothing = 0
    Update = 1
    Select = 1
