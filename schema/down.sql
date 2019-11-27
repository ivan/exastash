\set ON_ERROR_STOP off

DROP TRIGGER names_check_update ON names;
DROP TABLE names;
DROP DOMAIN linux_basename;
DROP TYPE storage_type;
DROP TRIGGER storage_internetarchive_check_update ON storage_internetarchive;
DROP TABLE storage_internetarchive;
DROP DOMAIN ia_pathname;
DROP DOMAIN ia_item;
DROP TABLE storage_gdrive;
DROP TABLE gdrive_chunk_sequences;
DROP DOMAIN crc32c;
DROP DOMAIN md5;
DROP TABLE gdrive_domains;
DROP DOMAIN gdrive_domain;
DROP TABLE storage_inline;
DROP TRIGGER inodes_check_update ON inodes;
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
DROP FUNCTION raise_exception;
