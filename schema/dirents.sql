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

CREATE TABLE dirents (
    -- Imagine REFERENCES inodes (ino) here, actually managed by our triggers
    parent    ino             NOT NULL,
    basename  linux_basename  NOT NULL,
    -- Imagine REFERENCES inodes (ino) here, actually managed by our triggers
    child     ino             NOT NULL CHECK (child != parent),
    -- TODO: ensure that child is not in any of parents

    PRIMARY KEY (parent, basename)
);
REVOKE TRUNCATE ON dirents FROM current_user;

CREATE OR REPLACE FUNCTION dirents_handle_insert() RETURNS trigger AS $$
DECLARE
    parent_type inode_type;
    child_type inode_type;
BEGIN
    parent_type := (SELECT type FROM inodes WHERE ino = NEW.parent);
    IF parent_type IS NULL THEN
        RAISE EXCEPTION 'parent ino=% does not exist in inodes', NEW.parent;
    END IF;
    IF parent_type != 'DIR' THEN
        RAISE EXCEPTION 'parent ino=% is not a DIR', NEW.parent;
    END IF;
    IF (SELECT ino FROM inodes WHERE ino = NEW.child) IS NULL THEN
        RAISE EXCEPTION 'child ino=% does not exist in inodes', NEW.child;
    END IF;

    child_type := (SELECT type FROM inodes WHERE ino = NEW.child);
    IF child_type = 'DIR' THEN
        UPDATE inodes SET child_dir_count = child_dir_count + 1 WHERE ino = NEW.parent;
        UPDATE inodes SET parent_ino = NEW.parent               WHERE ino = NEW.child;
    END IF;

    UPDATE inodes SET dirents_count = dirents_count + 1 WHERE ino = NEW.child;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION dirents_handle_delete() RETURNS trigger AS $$
DECLARE
    child_type inode_type;
BEGIN
    child_type := (SELECT type FROM inodes WHERE ino = OLD.child);
    IF child_type = 'DIR' THEN
        UPDATE inodes SET child_dir_count = child_dir_count - 1 WHERE ino = OLD.parent;
    END IF;

    UPDATE inodes SET dirents_count = dirents_count - 1 WHERE ino = OLD.child;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER dirents_handle_insert
    BEFORE INSERT ON dirents
    FOR EACH ROW
    EXECUTE FUNCTION dirents_handle_insert();

CREATE TRIGGER dirents_check_update
    BEFORE UPDATE ON dirents
    FOR EACH ROW
    EXECUTE FUNCTION raise_exception('cannot change parent, basename, or child');

CREATE TRIGGER dirents_check_delete
    BEFORE UPDATE ON dirents
    FOR EACH ROW
    EXECUTE FUNCTION dirents_handle_delete();
