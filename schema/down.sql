\set ON_ERROR_STOP off

DROP TABLE names;
DROP DOMAIN linux_basename;
DROP TABLE storage_map;
DROP TYPE storage_type;
DROP TABLE storage_inline_content;
DROP TABLE storage_gdrive_chunks;
DROP DOMAIN crc32c;
DROP DOMAIN md5;
DROP INDEX inode_mtime_index;
DROP INDEX inode_size_index;
DROP TABLE inodes;
DROP DOMAIN symlink_pathname;
DROP TYPE inode_type;
DROP CAST (timestamp with time zone AS timespec64);
DROP FUNCTION timestamp_to_timespec64;
DROP TYPE timespec64;
DROP DOMAIN nsec;
DROP DOMAIN sec;
DROP FUNCTION count_not_nulls;
