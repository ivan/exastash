CREATE DATABASE IF NOT EXISTS ${DB_PREFIX + stashName};

CREATE TABLE IF NOT EXISTS inodes (
	id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
	type ENUM('f', 'd') NOT NULL,
	nlinks INT UNSIGNED NOT NULL,
	size BIGINT UNSIGNED NOT NULL,
	content BLOB,
	key_128 BINARY(16),
	key_256 BINARY(32),
	mtime DATETIME(6) NOT NULL,
	executable BOOLEAN,
	block_size MEDIUMINT UNSIGNED,
	added_time DATETIME(6) NOT NULL,
	added_user VARCHAR(32) NOT NULL,
	added_host VARCHAR(64) NOT NULL,
	added_version VARCHAR(42) NOT NULL
);

// Must use VARBINARY instead of VARCHAR because else we get
// "Error: Specified key was too long; max key length is 767 bytes"
// (255 chars can be up to 1020 bytes with utf8mb4)
CREATE TABLE IF NOT EXISTS names (
	parent BIGINT UNSIGNED NOT NULL,
	name VARBINARY(255) NOT NULL,
	child BIGINT UNSIGNED NOT NULL,
	PRIMARY KEY (parent, name)
);

CREATE TABLE IF NOT EXISTS chunks (
	id BIGINT UNSIGNED NOT NULL,
	idx INT UNSIGNED NOT NULL,
	file_id VARCHAR(255) NOT NULL,
	md5 BINARY(16),
	crc32c BINARY(4) NOT NULL,
	size BIGINT UNSIGNED NOT NULL,
	PRIMARY KEY (id, idx)
);
