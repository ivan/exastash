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
        AND VALUE != '.'
        AND VALUE != '..'
    );

CREATE TABLE dirents (
    parent        bigint          NOT NULL REFERENCES dirs (id),
    -- Exactly one of these
    child_dir     bigint          CHECK (child_dir     != parent) REFERENCES dirs (id),
    child_file    bigint          CHECK (child_file    != parent) REFERENCES files (id),
    child_symlink bigint          CHECK (child_symlink != parent) REFERENCES symlinks (id),
    -- Keep this last to reduce pg_column_size
    basename      linux_basename  NOT NULL,

    -- Ensure exactly one type of child is set
    CHECK (num_nonnulls(child_dir, child_file, child_symlink) = 1),

    PRIMARY KEY (parent, basename)
);

-- dirents REFERENCES dirs/files/symlinks tables and we may want to delete rows
-- from those tables, so we need indexes to avoid full table scans of dirents.
--
-- UNIQUE because a directory cannot have more than one parent
CREATE UNIQUE INDEX dirents_child_dir_index     ON dirents (child_dir);
CREATE        INDEX dirents_child_file_index    ON dirents (child_file);
CREATE        INDEX dirents_child_symlink_index ON dirents (child_symlink);

CREATE TRIGGER dirents_check_update
    BEFORE UPDATE ON dirents
    FOR EACH ROW
    EXECUTE FUNCTION raise_exception('cannot change parent, basename, or child_*');

CREATE TRIGGER dirents_forbid_truncate
    BEFORE TRUNCATE ON dirents
    EXECUTE FUNCTION raise_exception('truncate is forbidden');
