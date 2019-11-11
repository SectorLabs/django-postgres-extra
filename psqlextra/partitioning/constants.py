# comment placed on partition tables created by the partitioner
# partition tables that do not have this comment will _never_
# be deleted by the partitioner, this is a safety mechanism so
# manually created partitions aren't accidently cleaned up
AUTO_PARTITIONED_COMMENT = "psqlextra_auto_partitioned"


__all__ = ["AUTO_PARTITIONED_COMMENT"]
