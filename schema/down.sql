\set ON_ERROR_STOP off

DROP INDEX inode_mtime_index;
DROP INDEX inode_size_index;
DROP TABLE inodes;

DROP TYPE timespec64;
DROP TYPE inode_type;
DROP DOMAIN symlink_target;
DROP DOMAIN linux_filename;
DROP DOMAIN nsec;
DROP DOMAIN sec;
DROP FUNCTION count_not_nulls;
