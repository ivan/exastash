\set ON_ERROR_STOP off

DROP TABLE names;

DROP INDEX inode_mtime_index;
DROP INDEX inode_size_index;
DROP TABLE inodes;

DROP DOMAIN symlink_target;
DROP DOMAIN linux_basename;
DROP FUNCTION timestamp_to_timespec64;
DROP FUNCTION count_not_nulls;
DROP TYPE timespec64 CASCADE;
DROP TYPE inode_type CASCADE;
DROP DOMAIN nsec;
DROP DOMAIN sec;
