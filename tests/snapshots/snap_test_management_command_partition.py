# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots["test_management_command_partition_dry_run[-d] 1"] = (
    """test:
  - tobedeleted
  + tobecreated

1 partitions will be deleted
1 partitions will be created
""",
    "",
)

snapshots["test_management_command_partition_dry_run[--dry] 1"] = (
    """test:
  - tobedeleted
  + tobecreated

1 partitions will be deleted
1 partitions will be created
""",
    "",
)

snapshots["test_management_command_partition_auto_confirm[-y] 1"] = (
    """test:
  - tobedeleted
  + tobecreated

1 partitions will be deleted
1 partitions will be created
Operations applied.
""",
    "",
)

snapshots["test_management_command_partition_auto_confirm[--yes] 1"] = (
    """test:
  - tobedeleted
  + tobecreated

1 partitions will be deleted
1 partitions will be created
Operations applied.
""",
    "",
)

snapshots["test_management_command_partition_confirm_yes[y] 1"] = (
    """test:
  - tobedeleted
  + tobecreated

1 partitions will be deleted
1 partitions will be created
Do you want to proceed? (y/N) Operations applied.
""",
    "",
)

snapshots["test_management_command_partition_confirm_yes[Y] 1"] = (
    """test:
  - tobedeleted
  + tobecreated

1 partitions will be deleted
1 partitions will be created
Do you want to proceed? (y/N) Operations applied.
""",
    "",
)

snapshots["test_management_command_partition_confirm_yes[yes] 1"] = (
    """test:
  - tobedeleted
  + tobecreated

1 partitions will be deleted
1 partitions will be created
Do you want to proceed? (y/N) Operations applied.
""",
    "",
)

snapshots["test_management_command_partition_confirm_yes[YES] 1"] = (
    """test:
  - tobedeleted
  + tobecreated

1 partitions will be deleted
1 partitions will be created
Do you want to proceed? (y/N) Operations applied.
""",
    "",
)

snapshots["test_management_command_partition_confirm_no[n] 1"] = (
    """test:
  - tobedeleted
  + tobecreated

1 partitions will be deleted
1 partitions will be created
Do you want to proceed? (y/N) Operation aborted.
""",
    "",
)

snapshots["test_management_command_partition_confirm_no[N] 1"] = (
    """test:
  - tobedeleted
  + tobecreated

1 partitions will be deleted
1 partitions will be created
Do you want to proceed? (y/N) Operation aborted.
""",
    "",
)

snapshots["test_management_command_partition_confirm_no[no] 1"] = (
    """test:
  - tobedeleted
  + tobecreated

1 partitions will be deleted
1 partitions will be created
Do you want to proceed? (y/N) Operation aborted.
""",
    "",
)

snapshots["test_management_command_partition_confirm_no[No] 1"] = (
    """test:
  - tobedeleted
  + tobecreated

1 partitions will be deleted
1 partitions will be created
Do you want to proceed? (y/N) Operation aborted.
""",
    "",
)

snapshots["test_management_command_partition_confirm_no[NO] 1"] = (
    """test:
  - tobedeleted
  + tobecreated

1 partitions will be deleted
1 partitions will be created
Do you want to proceed? (y/N) Operation aborted.
""",
    "",
)
