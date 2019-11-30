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
REVOKE TRUNCATE ON names FROM current_user;

CREATE TRIGGER names_check_update
    BEFORE UPDATE ON names
    FOR EACH ROW
    EXECUTE FUNCTION raise_exception('cannot change parent, name, or child');
