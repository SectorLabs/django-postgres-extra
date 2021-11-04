# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import GenericRepr, Snapshot


snapshots = Snapshot()

snapshots['test_management_command_partition_auto_confirm[--yes] 1'] = GenericRepr("CaptureResult(out='test:\\n  - tobedeleted\\n  + tobecreated\\n\\n1 partitions will be deleted\\n1 partitions will be created\\nOperations applied.\\n', err='')")

snapshots['test_management_command_partition_auto_confirm[-y] 1'] = GenericRepr("CaptureResult(out='test:\\n  - tobedeleted\\n  + tobecreated\\n\\n1 partitions will be deleted\\n1 partitions will be created\\nOperations applied.\\n', err='')")

snapshots['test_management_command_partition_confirm_no[NO] 1'] = GenericRepr("CaptureResult(out='test:\\n  - tobedeleted\\n  + tobecreated\\n\\n1 partitions will be deleted\\n1 partitions will be created\\nDo you want to proceed? (y/N) Operation aborted.\\n', err='')")

snapshots['test_management_command_partition_confirm_no[N] 1'] = GenericRepr("CaptureResult(out='test:\\n  - tobedeleted\\n  + tobecreated\\n\\n1 partitions will be deleted\\n1 partitions will be created\\nDo you want to proceed? (y/N) Operation aborted.\\n', err='')")

snapshots['test_management_command_partition_confirm_no[No] 1'] = GenericRepr("CaptureResult(out='test:\\n  - tobedeleted\\n  + tobecreated\\n\\n1 partitions will be deleted\\n1 partitions will be created\\nDo you want to proceed? (y/N) Operation aborted.\\n', err='')")

snapshots['test_management_command_partition_confirm_no[n] 1'] = GenericRepr("CaptureResult(out='test:\\n  - tobedeleted\\n  + tobecreated\\n\\n1 partitions will be deleted\\n1 partitions will be created\\nDo you want to proceed? (y/N) Operation aborted.\\n', err='')")

snapshots['test_management_command_partition_confirm_no[no] 1'] = GenericRepr("CaptureResult(out='test:\\n  - tobedeleted\\n  + tobecreated\\n\\n1 partitions will be deleted\\n1 partitions will be created\\nDo you want to proceed? (y/N) Operation aborted.\\n', err='')")

snapshots['test_management_command_partition_confirm_yes[YES] 1'] = GenericRepr("CaptureResult(out='test:\\n  - tobedeleted\\n  + tobecreated\\n\\n1 partitions will be deleted\\n1 partitions will be created\\nDo you want to proceed? (y/N) Operations applied.\\n', err='')")

snapshots['test_management_command_partition_confirm_yes[Y] 1'] = GenericRepr("CaptureResult(out='test:\\n  - tobedeleted\\n  + tobecreated\\n\\n1 partitions will be deleted\\n1 partitions will be created\\nDo you want to proceed? (y/N) Operations applied.\\n', err='')")

snapshots['test_management_command_partition_confirm_yes[y] 1'] = GenericRepr("CaptureResult(out='test:\\n  - tobedeleted\\n  + tobecreated\\n\\n1 partitions will be deleted\\n1 partitions will be created\\nDo you want to proceed? (y/N) Operations applied.\\n', err='')")

snapshots['test_management_command_partition_confirm_yes[yes] 1'] = GenericRepr("CaptureResult(out='test:\\n  - tobedeleted\\n  + tobecreated\\n\\n1 partitions will be deleted\\n1 partitions will be created\\nDo you want to proceed? (y/N) Operations applied.\\n', err='')")

snapshots['test_management_command_partition_dry_run[--dry] 1'] = GenericRepr("CaptureResult(out='test:\\n  - tobedeleted\\n  + tobecreated\\n\\n1 partitions will be deleted\\n1 partitions will be created\\n', err='')")

snapshots['test_management_command_partition_dry_run[-d] 1'] = GenericRepr("CaptureResult(out='test:\\n  - tobedeleted\\n  + tobecreated\\n\\n1 partitions will be deleted\\n1 partitions will be created\\n', err='')")
