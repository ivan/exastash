CREATE DATABASE IF NOT EXISTS ${DB_PREFIX + stashName};

CREATE TYPE inode_type AS ENUM ('f', 'd');

CREATE DOMAIN int_nlinks AS integer CHECK(VALUE >= 1);
CREATE DOMAIN bigint_size AS bigint CHECK(VALUE >= 0);
CREATE DOMAIN bytea_key_128 AS bytea CHECK(octet_length(VALUE) = 16);
CREATE DOMAIN bytea_key_256 AS bytea CHECK(octet_length(VALUE) = 32);
CREATE DOMAIN int_block_size AS integer CHECK(VALUE >= 1);

-- TODO: packs table
-- Each file still has its own key

CREATE TABLE inodes (
	id bigserial NOT NULL PRIMARY KEY,
	type inode_type NOT NULL,
	nlinks int_nlinks NOT NULL,
	size bigint_size NOT NULL,
	-- Do I even need inline files? I could pack even tiny files and have a read cache
	-- I think I need to support it for the millions of existing tiny files I have
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

-- Filenames must be valid UTF-8 and <= 255 bytes
-- Filenames are case-sensitive and are not unicode-normalized
CREATE DOMAIN text_filename AS text CHECK(octet_length(VALUE) <= 255 AND VALUE !~ '/');

CREATE TABLE names (
	parent bigint NOT NULL REFERENCES inodes (id),
	name text_filename NOT NULL,
	child bigint NOT NULL REFERENCES inodes (id),
	PRIMARY KEY (parent, name),
	-- Just a sanity check; this doesn't guarantee that there aren't any deeper loops
	CONSTRAINT child_ne_parent CHECK (parent <> child)
);

CREATE TYPE pack_store_type AS ENUM ('gdrive');
CREATE DOMAIN bigint_pack_size AS bigint CHECK(VALUE >= 1);

-- What used to be chunk-stores.json in node terastash
CREATE TABLE pack_store (
	name text NOT NULL PRIMARY KEY,
	type pack_store_type NOT NULL,
	pack_size bigint_pack_size NOT NULL,
	-- The client_id of the Google API application to use; frequently shared across pack stores
	client_id text,
	-- The client_secret of the Google API application to use; frequently shared across pack stores
	client_secret text,
	folder text
);

CREATE DOMAIN text_email AS text CHECK(VALUE ~ '@');
CREATE TYPE oauth2_token_type AS ENUM ('bearer', 'mac');

-- What used to be google-tokens.json in node terastash
-- We support using multiple Google accounts to upload to a pack store
CREATE TABLE oauth2_credentials (
	email_address text_email NOT NULL,
	client_id text NOT NULL REFERENCES pack_store,
	-- The spec has no max size for the access or refresh token
	access_token text NOT NULL,
	refresh_token text NOT NULL,
	token_type oauth2_token_type NOT NULL,
	expiry_date timestamp without time zone NOT NULL,
	PRIMARY KEY (email_address, client_id)
);

CREATE TABLE packs (
	-- TODO

	-- Because we can only delete using the same account we uploaded from
	email_address REFERENCES oauth2_credentials
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
