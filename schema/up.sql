-- https://stackoverflow.com/questions/15178859/postgres-constraint-ensuring-one-column-of-many-is-present
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
    ino             bigserial       NOT NULL PRIMARY KEY,
    size            bigint          NOT NULL CHECK (size >= 0),
    mtime           timespec64      NOT NULL,
    executable      boolean         NOT NULL,
    inline_content  bytea,
    sylink_target   symlink_target,

    -- CHECK (count_not_nulls(array[inline_id, gdrive_id]) = 1),
    CONSTRAINT size_matches_inline_content CHECK (inline_content IS NULL OR octet_length(inline_content) = size)
);
