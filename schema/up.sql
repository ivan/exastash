\set ON_ERROR_STOP on

CREATE FUNCTION raise_exception() RETURNS trigger
AS $$
DECLARE
    message text;
BEGIN
    message := TG_ARGV[0];
    RAISE EXCEPTION '%', message;
END;
$$ LANGUAGE plpgsql;



CREATE DOMAIN sec  AS bigint CHECK (VALUE >= 0);
CREATE DOMAIN nsec AS bigint CHECK (VALUE >= 0 AND VALUE <= 10 ^ 9);

-- We store timespec64 instead of `timestamp with time zone` because
-- `timestamp with time zone` is only microsecond precise, and some
-- applications may reasonably expect nanosecond-precise mtimes to
-- round trip correctly.  It may also be useful in some cases when
-- sorting files created at nearly the same time.
CREATE TYPE timespec64 AS (
    sec  sec,
    nsec nsec
);

CREATE FUNCTION timestamp_to_timespec64(timestamp with time zone) RETURNS timespec64 AS $$
DECLARE
    epoch numeric;
BEGIN
    -- epoch: "For timestamp with time zone values, the number of seconds since
    -- 1970-01-01 00:00:00 UTC (can be negative)"
    --
    -- Convert to numeric for % 1 below
    epoch := extract(epoch from $1)::numeric;
    RETURN(SELECT (
        -- integer part
        floor(epoch),
        -- decimal part, times the number of nanoseconds in a second
        (epoch % 1) * 10 ^ 9
    )::timespec64);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE CAST (timestamp with time zone AS timespec64) WITH FUNCTION timestamp_to_timespec64 AS ASSIGNMENT;



-- http://man7.org/linux/man-pages/man7/inode.7.html
CREATE TYPE inode_type AS ENUM ('REG', 'DIR', 'LNK');

-- text instead of bytea, see the UTF-8 rationale on linux_basename
CREATE DOMAIN symlink_pathname AS text
    -- ext4 and btrfs limit the symlink target to ~4096 bytes.
    -- xfs limits the symlink target to 1024 bytes.
    -- We follow the lower limit in case symlinks need to be copied to XFS.
    --
    -- Linux does not allow empty pathnames: https://lwn.net/Articles/551224/
    CHECK (octet_length(VALUE) >= 1 AND octet_length(VALUE) <= 1024);

-- We don't store uid, gid, and the exact mode; those can be decided and
-- changed globally by the user.
CREATE TABLE inodes (
    ino                 bigserial         NOT NULL PRIMARY KEY CHECK (ino >= 2),
    type                inode_type        NOT NULL,
    size                bigint            CHECK (size >= 0),
    mtime               timespec64        NOT NULL,
    executable          boolean,
    symlink_target      symlink_pathname,

    CONSTRAINT only_reg_has_size           CHECK ((type != 'REG' AND size           IS NULL) OR (type = 'REG' AND size           IS NOT NULL)),
    CONSTRAINT only_reg_has_executable     CHECK ((type != 'REG' AND executable     IS NULL) OR (type = 'REG' AND executable     IS NOT NULL)),
    CONSTRAINT only_lnk_has_symlink_target CHECK ((type != 'LNK' AND symlink_target IS NULL) OR (type = 'LNK' AND symlink_target IS NOT NULL))
);
-- TODO: use trigger to make sure inode is a child of some parent?

CREATE INDEX inode_size_index  ON inodes (size);
CREATE INDEX inode_mtime_index ON inodes (mtime);

CREATE TRIGGER inodes_check_update
    BEFORE UPDATE ON inodes
    FOR EACH ROW
    WHEN (
        OLD.ino != NEW.ino OR
        OLD.type != NEW.type OR
        OLD.symlink_target IS DISTINCT FROM NEW.symlink_target
    )
    EXECUTE FUNCTION raise_exception('cannot change ino, type, or symlink_target');

INSERT INTO inodes (ino, type, mtime) VALUES (2, 'DIR', now()::timespec64);

-- inode 0 is not used by Linux filesystems (0 means NULL).
-- inode 1 is used by Linux filesystems for bad blocks information.
-- inode 2 is used directly above for /
-- Start with inode 3 for all other inodes.
ALTER SEQUENCE inodes_ino_seq RESTART WITH 3;



CREATE TABLE storage_inline (
    ino      bigint  NOT NULL PRIMARY KEY REFERENCES inodes,
    content  bytea   NOT NULL
);

CREATE DOMAIN gdrive_domain AS text
    CHECK (length(VALUE) >= 1 AND length(VALUE) <= 255);

CREATE TABLE gdrive_domains (
    gdrive_domain  gdrive_domain  NOT NULL PRIMARY KEY
    -- TODO: access keys
);

CREATE DOMAIN md5    AS bytea CHECK (length(VALUE) = 16);
CREATE DOMAIN crc32c AS bytea CHECK (length(VALUE) = 4);

CREATE TABLE gdrive_chunk_sequences (
    chunk_sequence  bigserial  NOT NULL CHECK (chunk_sequence >= 1),
    chunk_number    smallint   NOT NULL CHECK (chunk_number >= 0 and chunk_number < chunk_total),
    chunk_total     smallint   NOT NULL CHECK (chunk_total >= 1),
    -- the shortest file_id we have is 28
    -- the longest file_id we have is 33, but allow up to 160 in case Google changes format
    file_id         text       NOT NULL CHECK (file_id ~ '\A[-_0-9A-Za-z]{28,160}\Z'),
    -- forbid very long account names
    account         text       CHECK (account ~ '\A.{1,255}\Z'), -- some of our old chunks have no account
    md5             md5        NOT NULL,
    crc32c          crc32c     NOT NULL,
    size            bigint     NOT NULL CHECK (size >= 1),

    PRIMARY KEY (chunk_sequence, chunk_number)
);
-- TODO: represent ordered list better and/or add trigger to make sure there are no holes

-- There can be multiple chunks in a sequence of chunks
CREATE TABLE storage_gdrive (
    ino             bigint         NOT NULL REFERENCES inodes,
    gdrive_domain   gdrive_domain  NOT NULL REFERENCES gdrive_domains,
    chunk_sequence  bigint         NOT NULL,

    -- Include chunk_sequence in the key because we might want to reupload
    -- some chunk sequences in a new format.
    PRIMARY KEY (ino, gdrive_domain, chunk_sequence)
);

CREATE DOMAIN ia_item AS text
    CHECK (
        -- https://help.archive.org/hc/en-us/articles/360018818271-Internet-Archive-Metadata
        VALUE ~ '\A[A-Za-z0-9][-_\.A-Za-z0-9]{0,99}\Z'
    );

CREATE DOMAIN ia_pathname AS text
    CHECK (
        octet_length(VALUE) >= 1 AND
        octet_length(VALUE) <= 1024 -- the true maximum is unknown
    );

CREATE TABLE storage_internetarchive (
    ino           bigint                    NOT NULL REFERENCES inodes,
    ia_item       ia_item                   NOT NULL,
    pathname      ia_pathname               NOT NULL,
    darked        boolean                   NOT NULL DEFAULT false,
    last_probed   timestamp with time zone,

    -- We may know of more than one item has the file.
    PRIMARY KEY (ino, ia_item)
);

CREATE TRIGGER storage_internetarchive_check_update
    BEFORE UPDATE ON storage_internetarchive
    FOR EACH ROW
    WHEN (
        OLD.ino != NEW.ino OR
        OLD.ia_item != NEW.ia_item OR
        OLD.pathname != NEW.pathname
    )
    EXECUTE FUNCTION raise_exception('cannot change ino, ia_item, or pathname');

CREATE TYPE storage_type AS ENUM ('inline', 'gdrive', 'internetarchive');

-- An inode can be stored in 1 or more storage location, including multiple
-- of the same type of storage.
--
-- TODO: trigger to ensure ino is of type REG




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
CREATE DOMAIN linux_basename AS text
    CHECK (
        octet_length(VALUE) >= 1 AND
        octet_length(VALUE) <= 255
        AND VALUE !~ '/'
    );

CREATE TABLE names (
    parent  bigint          NOT NULL REFERENCES inodes (ino),
    name    linux_basename  NOT NULL,
    child   bigint          NOT NULL REFERENCES inodes (ino),

    PRIMARY KEY (parent, name)
    -- TODO ensure that child is not any of parents
);

CREATE TRIGGER names_check_update
    BEFORE UPDATE ON names
    FOR EACH ROW
    EXECUTE FUNCTION raise_exception('cannot change parent, name, or child');
