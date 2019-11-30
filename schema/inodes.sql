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
    ino                 bigint            GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY CHECK (ino >= 2),
    type                inode_type        NOT NULL,
    size                bigint            CHECK (size >= 0),
    mtime               timespec64        NOT NULL,
    executable          boolean,
    symlink_target      symlink_pathname,

    CONSTRAINT only_reg_has_size           CHECK ((type != 'REG' AND size           IS NULL) OR (type = 'REG' AND size           IS NOT NULL)),
    CONSTRAINT only_reg_has_executable     CHECK ((type != 'REG' AND executable     IS NULL) OR (type = 'REG' AND executable     IS NOT NULL)),
    CONSTRAINT only_lnk_has_symlink_target CHECK ((type != 'LNK' AND symlink_target IS NULL) OR (type = 'LNK' AND symlink_target IS NOT NULL))
);
REVOKE TRUNCATE ON inodes FROM current_user;
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
ALTER TABLE inodes ALTER COLUMN ino RESTART WITH 3;

CREATE OR REPLACE FUNCTION assert_inode_is_regular_file() RETURNS trigger AS $$
DECLARE
    ino_type inode_type;
BEGIN
    ino_type := (SELECT type FROM inodes WHERE ino = NEW.ino);
    IF ino_type != 'REG' THEN
        RAISE EXCEPTION 'inode % is %, not a regular file', NEW.ino, ino_type;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;