\set ON_ERROR_STOP on

-- https://stackoverflow.com/questions/15178859/postgres-constraint-ensuring-one-column-of-many-is-present
-- Usage: CHECK (count_not_nulls(array[inline_id, gdrive_id]) = 1),
CREATE FUNCTION count_not_nulls(p_array anyarray)
RETURNS BIGINT AS
$$
    SELECT count(x) FROM unnest($1) AS x
$$ LANGUAGE SQL IMMUTABLE;

CREATE DOMAIN sec  AS bigint CHECK (VALUE >= 0);
CREATE DOMAIN nsec AS bigint CHECK (VALUE >= 0 AND VALUE <= 10 ^ 9);

CREATE TYPE timespec64 AS (
    sec  sec,
    nsec nsec
);

-- http://man7.org/linux/man-pages/man7/inode.7.html
CREATE TYPE inode_type AS ENUM ('REG', 'DIR', 'LNK');

-- We do not follow Windows filename restrictions here because they are
-- often very restrictive (e.g. no "aux.c.something"); applications can
-- still use Windows-compatible filenames if they wish, but this is not
-- enforced here.
--
-- We do enforce UTF-8 here because the need for non-UTF-8 filenames is
-- now very rare, and supporting them would complicate both the Rust
-- implementation and display logic.
--
-- Windows and macOS allow basenames to have up to 255 UTF-16 codepoints,
-- but we mostly run Linux and need to follow its more restrictive limit
-- of 255 bytes.
CREATE DOMAIN linux_filename AS text
    CHECK (
        octet_length(VALUE) <= 255
        AND VALUE !~ '/'
    );

-- text instead of bytea, see the UTF-8 rationale for linux_filename
CREATE DOMAIN symlink_target AS text
    -- ext4 and btrfs limit the symlink target to ~4096 bytes
    -- xfs limits the symlink target to 1024 bytes
    -- We follow the lower limit in case symlinks need to be copied to XFS
    CHECK (octet_length(VALUE) <= 1024);

-- We don't store uid, gid, and the exact mode; those can be decided and
-- changed globally by the user.
CREATE TABLE inodes (
    ino             bigserial       NOT NULL PRIMARY KEY CHECK (ino >= 0),
    type            inode_type      NOT NULL,
    size            bigint          CHECK (size >= 0),
    mtime           timespec64      NOT NULL,
    executable      boolean,
    inline_content  bytea,
    symlink_target  symlink_target,

    CONSTRAINT only_reg_has_size                 CHECK ((type != 'REG' AND size           IS NULL) OR (type = 'REG' AND size           IS NOT NULL)),
    CONSTRAINT only_reg_has_executable           CHECK ((type != 'REG' AND executable     IS NULL) OR (type = 'REG' AND executable     IS NOT NULL)),
    CONSTRAINT only_reg_maybe_has_inline_content CHECK (inline_content IS NULL OR type = 'REG'),
    CONSTRAINT only_lnk_has_symlink_target       CHECK ((type != 'LNK' AND symlink_target IS NULL) OR (type = 'LNK' AND symlink_target IS NOT NULL)),
    CONSTRAINT size_matches_inline_content       CHECK (inline_content IS NULL OR size = octet_length(inline_content))
);

-- inode 0 is not used by Linux filesystems
-- inode 1 is used by Linux filesystems for bad blocks information
-- Start with inode 2 to avoid confusing any stupid software
ALTER SEQUENCE inodes_ino_seq RESTART WITH 2;

CREATE INDEX inode_size_index  ON inodes (size);
CREATE INDEX inode_mtime_index ON inodes (mtime);
