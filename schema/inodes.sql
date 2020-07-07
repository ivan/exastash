-- text instead of bytea, see the UTF-8 rationale on linux_basename
CREATE DOMAIN symlink_pathname AS text
    -- ext4 and btrfs limit the symlink target to ~4096 bytes.
    -- xfs limits the symlink target to 1024 bytes.
    -- We follow the lower limit in case symlinks need to be copied to XFS.
    --
    -- Linux does not allow empty pathnames: https://lwn.net/Articles/551224/
    CHECK (octet_length(VALUE) >= 1 AND octet_length(VALUE) <= 1024);

CREATE DOMAIN hostname AS text CHECK (octet_length(VALUE) <= 253);

-- Instead of occupying an entire 64-bit inode space in these tables, we store smaller ids that
-- can be mapped into a 64-bit inode range by e.g. a FUSE server serving a filesystem.
-- This allows such a server to map other id spaces into the inode range if needed.
--
-- We don't store uid, gid, and the exact mode; those can be decided and changed globally by
-- the user.
--
-- We use timestamptz here which is only microsecond-precise.  xfs and ext4 on Linux are
-- nanosecond-precise and NTFS on Windows is 100ns-precise; timestamptz won't allow the last 3
-- digits on nanoseconds to round trip, but it isn't worth storing extra bytes to support this.
--
-- Columns are ordered for optimal packing, be careful.
CREATE TABLE dirs (
    -- Limit of 2T can be raised if needed
    id              bigint            GENERATED ALWAYS AS IDENTITY PRIMARY KEY CHECK (id >= 1 AND id < 2000000000000),
    mtime           timestamptz       NOT NULL,
    birth_time      timestamptz       NOT NULL,
    -- When/where/with what exastash version was this inode produced?
    birth_version   smallint          NOT NULL REFERENCES exastash_versions (id),
    birth_hostname  hostname          NOT NULL

    -- A CONSTRAINT is added to this table in dirents.sql
);
-- This should always get id=1
INSERT INTO dirs VALUES (DEFAULT, now(), now(), 41, '');

CREATE TABLE files (
    -- Limit of 2T can be raised if needed
    id              bigint            GENERATED ALWAYS AS IDENTITY PRIMARY KEY CHECK (id >= 1 AND id < 2000000000000),
    size            bigint            NOT NULL CHECK (size >= 0),
    mtime           timestamptz       NOT NULL,
    birth_time      timestamptz       NOT NULL,
    birth_version   smallint          NOT NULL REFERENCES exastash_versions (id),
    executable      boolean           NOT NULL,
    birth_hostname  hostname          NOT NULL
);

CREATE TABLE symlinks (
    -- Limit of 2T can be raised if needed
    id              bigint            GENERATED ALWAYS AS IDENTITY PRIMARY KEY CHECK (id >= 1 AND id < 2000000000000),
    mtime           timestamptz       NOT NULL,
    birth_time      timestamptz       NOT NULL,
    birth_version   smallint          NOT NULL REFERENCES exastash_versions (id),
    target          symlink_pathname  NOT NULL,
    birth_hostname  hostname          NOT NULL
);


-- We limit inserts to one dir at a time. Because a dir must be a parent of some
-- existing directory, this prevents the creation of cycles like an A->B, B->A
-- not connected to the root dir.
CREATE OR REPLACE FUNCTION dirs_check_insert() RETURNS trigger AS $$
DECLARE
    transaction_has_dir_insert text;
BEGIN
    transaction_has_dir_insert := current_setting('stash.transaction_has_dir_insert', /* missing_ok */true);
    IF transaction_has_dir_insert = '1' THEN
        RAISE EXCEPTION 'cannot insert more than one dir per transaction';
    END IF;
    PERFORM set_config('stash.transaction_has_dir_insert', '1', /* transaction_local */true);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER dirs_check_insert
    AFTER INSERT ON dirs
    FOR EACH ROW
    EXECUTE FUNCTION dirs_check_insert();


CREATE TRIGGER dirs_check_update
    BEFORE UPDATE ON dirs
    FOR EACH ROW
    WHEN (
        OLD.id             != NEW.id             OR
        OLD.birth_time     != NEW.birth_time     OR
        OLD.birth_version  != NEW.birth_version  OR
        OLD.birth_hostname != NEW.birth_hostname
    )
    EXECUTE FUNCTION raise_exception('cannot change id or birth_*');

CREATE TRIGGER files_check_update
    BEFORE UPDATE ON files
    FOR EACH ROW
    WHEN (
        OLD.id             != NEW.id             OR
        OLD.birth_time     != NEW.birth_time     OR
        OLD.birth_version  != NEW.birth_version  OR
        OLD.birth_hostname != NEW.birth_hostname
    )
    EXECUTE FUNCTION raise_exception('cannot change id or birth_*');

CREATE TRIGGER symlinks_check_update
    BEFORE UPDATE ON symlinks
    FOR EACH ROW
    WHEN (
        OLD.id             != NEW.id             OR
        OLD.target         != NEW.target         OR
        OLD.birth_time     != NEW.birth_time     OR
        OLD.birth_version  != NEW.birth_version  OR
        OLD.birth_hostname != NEW.birth_hostname
    )
    EXECUTE FUNCTION raise_exception('cannot change id, target, or birth_*');


CREATE TRIGGER dirs_forbid_truncate
    BEFORE TRUNCATE ON dirs
    EXECUTE FUNCTION raise_exception('truncate is forbidden');

CREATE TRIGGER files_forbid_truncate
    BEFORE TRUNCATE ON files
    EXECUTE FUNCTION raise_exception('truncate is forbidden');

CREATE TRIGGER symlinks_forbid_truncate
    BEFORE TRUNCATE ON symlinks
    EXECUTE FUNCTION raise_exception('truncate is forbidden');
