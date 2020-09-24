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
    parent         bigint          NOT NULL,
    child_dir      bigint          CHECK (child_dir != parent OR (parent = 1 AND child_dir = 1)),
    child_file     bigint,
    child_symlink  bigint,
    basename       linux_basename  NOT NULL,

    -- Ensure exactly one type of child is set
    CHECK (num_nonnulls(child_dir, child_file, child_symlink) = 1),

    CONSTRAINT dirents_child_dir_fkey     FOREIGN KEY (child_dir)     REFERENCES stash.dirs (id),
    CONSTRAINT dirents_child_file_fkey    FOREIGN KEY (child_file)    REFERENCES stash.files (id),
    CONSTRAINT dirents_child_symlink_fkey FOREIGN KEY (child_symlink) REFERENCES stash.symlinks (id),
    CONSTRAINT dirents_parent_fkey        FOREIGN KEY (parent)        REFERENCES stash.dirs (id),

    PRIMARY KEY (parent, basename)
);
INSERT INTO dirents VALUES (1, 1, NULL, NULL, 'the root directory is its own parent in dirents because the dirs table requires all dirs to be a child_dir of some dir');

-- dirents REFERENCES dirs/files/symlinks tables and we may want to delete rows
-- from those tables, so we need indexes to avoid full table scans of dirents.
--
-- UNIQUE INDEX on child_dir because a directory cannot have more than one parent.
-- "Hard linked" directories are not desired because there are no tools that
-- expect or handle them properly when recursively copying or deleting.
CREATE UNIQUE INDEX dirents_child_dir_index     ON dirents (child_dir);
CREATE        INDEX dirents_child_file_index    ON dirents (child_file);
CREATE        INDEX dirents_child_symlink_index ON dirents (child_symlink);

-- We limit inserts and deletes to one dirent at a time. Because a dir must be a
-- parent of some existing directory, this prevents the creation of cycles like an
-- A->B, B->A not connected to the root dir, or the re-parenting a directory to a
-- subdir of itself.
--
-- This must be on dirents instead of dirs because dirents is where cycles can
-- actually be created (including in a follow-up transaction to replace dirents
-- for some existing dirs).
CREATE OR REPLACE FUNCTION dirents_check_insert_or_delete() RETURNS trigger AS $$
DECLARE
    transaction_touched_dirent_dir_child text;
    unsafe_internal_dirent_creation text;
BEGIN
    -- file and symlink children cannot create cycles
    IF TG_OP = 'INSERT' AND NEW.child_dir IS NULL THEN
        RETURN NULL;
    ELSIF TG_OP = 'DELETE' AND OLD.child_dir IS NULL THEN
        RETURN NULL;
    END IF;

    -- escape hatch for populate-exastash and to-be-implemented directory move operation
    unsafe_internal_dirent_creation := current_setting('stash.unsafe_internal_dirent_creation', /* missing_ok */true);
    IF unsafe_internal_dirent_creation = '1' THEN
        RETURN NULL;
    END IF;

    transaction_touched_dirent_dir_child := current_setting('stash.transaction_touched_dirent_dir_child', /* missing_ok */true);
    IF transaction_touched_dirent_dir_child = '1' THEN
        RAISE EXCEPTION 'cannot insert or delete more than one dirent with a child_dir per transaction';
    END IF;
    PERFORM set_config('stash.transaction_touched_dirent_dir_child', '1', /* transaction_local */true);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER dirents_check_insert_or_delete
    AFTER INSERT OR DELETE ON dirents
    FOR EACH ROW
    EXECUTE FUNCTION dirents_check_insert_or_delete();

CREATE TRIGGER dirents_check_update
    BEFORE UPDATE ON dirents
    FOR EACH ROW
    EXECUTE FUNCTION raise_exception('cannot change parent, basename, or child_*');

CREATE TRIGGER dirents_forbid_truncate
    BEFORE TRUNCATE ON dirents
    EXECUTE FUNCTION raise_exception('truncate is forbidden');

ALTER TABLE dirs
    -- Use deferrable constraints because we mutually FK dirs to dirents. Do that
    -- because we do not want to allow directories to be orphaned in the tree.  Do
    -- that because orphaned directories can't be re-parented without scanning the
    -- entire child tree for potential cycles.
    ADD CONSTRAINT dirs_id_fkey FOREIGN KEY (id) REFERENCES dirents (child_dir) DEFERRABLE INITIALLY DEFERRED;
