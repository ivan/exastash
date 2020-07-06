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

-- Columns are ordered for optimal packing, be careful
CREATE TABLE dirents (
    parent        bigint          NOT NULL                    REFERENCES dirs (id),
    -- This CHECK constraint is just an early check that cannot prevent cycles
    child_dir     bigint          CHECK (child_dir != parent) REFERENCES dirs (id),
    child_file    bigint                                      REFERENCES files (id),
    child_symlink bigint                                      REFERENCES symlinks (id),
    basename      linux_basename  NOT NULL,

    -- Ensure exactly one type of child is set
    CHECK (num_nonnulls(child_dir, child_file, child_symlink) = 1),

    PRIMARY KEY (parent, basename)
);

-- dirents REFERENCES dirs/files/symlinks tables and we may want to delete rows
-- from those tables, so we need indexes to avoid full table scans of dirents.
--
-- UNIQUE INDEX on child_dir because a directory cannot have more than one parent.
-- While multiple parents could be desired in some cases, there are no tools that
-- expect or handle this properly when recursively copying or deleting.
--
-- Note that a unique index cannot prevent cycles from forming because constraints
-- can be deferred (e.g. INSERT A->B, B->A), so we must use a trigger to prevent cycles.
CREATE UNIQUE INDEX dirents_child_dir_index     ON dirents (child_dir);
CREATE        INDEX dirents_child_file_index    ON dirents (child_file);
CREATE        INDEX dirents_child_symlink_index ON dirents (child_symlink);

-- TODO make sure we lock rows for sharing - don't want to let concurrent transactions muck things up
CREATE OR REPLACE FUNCTION dirents_ensure_no_cycles() RETURNS trigger AS $$
DECLARE

BEGIN
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER dirents_check_insert
    BEFORE UPDATE ON dirents
    FOR EACH ROW
    EXECUTE FUNCTION dirents_ensure_no_cycles();

CREATE TRIGGER dirents_check_update
    BEFORE UPDATE ON dirents
    FOR EACH ROW
    EXECUTE FUNCTION raise_exception('cannot change parent, basename, or child_*');

CREATE TRIGGER dirents_forbid_truncate
    BEFORE TRUNCATE ON dirents
    EXECUTE FUNCTION raise_exception('truncate is forbidden');
