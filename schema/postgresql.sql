CREATE DATABASE IF NOT EXISTS ${DB_PREFIX + stashName};

CREATE TYPE inode_type AS ENUM ('f', 'd');

CREATE DOMAIN int_nlinks AS integer CHECK(VALUE >= 1);
CREATE DOMAIN int_size AS bigint CHECK(VALUE >= 0);
CREATE DOMAIN bytea_key_128 AS bytea CHECK(octet_length(VALUE) = 16);
CREATE DOMAIN bytea_key_256 AS bytea CHECK(octet_length(VALUE) = 32);
CREATE DOMAIN int_block_size AS integer CHECK(VALUE >= 1);

-- TODO: packs table
-- Each file still has its own key

CREATE TABLE inodes (
	id bigserial NOT NULL PRIMARY KEY,
	type inode_type NOT NULL,
	nlinks int_nlinks NOT NULL,
	size int_size NOT NULL,
	content_lz4 bytea,
	key_128 bytea_key_128,
	key_256 bytea_key_256,
	mtime timestamp without time zone NOT NULL,
	executable boolean,
	-- AES-GCM block size.  TODO: move to pack table
	-- If null, it's AES-CTR instead of AES-GCM
	block_size int_block_size,
	added_time timestamp without time zone NOT NULL,
	added_user character varying(32) NOT NULL,
	added_host character varying(64) NOT NULL,
	added_version character varying(42) NOT NULL
	-- TODO: constraint: if file, executable = TRUE or FALSE, if dir, executable = NULL
	-- TODO: constraint: if dir, content_lz4 = NULL
);

CREATE DOMAIN text_filename AS text CHECK(octet_length(VALUE) <= 255 AND VALUE !~ '/');

CREATE TABLE names (
	parent bigint NOT NULL REFERENCES inodes (id),
	name text_filename NOT NULL,
	child bigint NOT NULL REFERENCES inodes (id),
	PRIMARY KEY (parent, name),
	CONSTRAINT child_ne_parent CHECK (parent <> child)
);

CREATE TABLE chunks (
	id BIGINT UNSIGNED NOT NULL,
	idx INT UNSIGNED NOT NULL,
	file_id VARCHAR(255) NOT NULL,
	md5 BINARY(16),
	crc32c BINARY(4) NOT NULL,
	size BIGINT UNSIGNED NOT NULL,
	PRIMARY KEY (id, idx)
);
