CREATE PROCEDURE create_root_inode(hostname text, exastash_version integer)
LANGUAGE SQL
AS $$
    INSERT INTO dirs (ino, mtime, birth_time, birth_hostname, birth_version)
        VALUES (2, now(), now(), hostname, exastash_version);
$$;

CREATE PROCEDURE remove_dirent(parent_ bigint, basename_ linux_basename)
LANGUAGE SQL
AS $$
    DELETE FROM dirents WHERE parent = parent_ AND basename = basename_;
$$;

CREATE OR REPLACE FUNCTION __get_ino_for_path(current_ino bigint, path text, symlink_resolutions_left int) RETURNS RECORD AS $$
DECLARE
    segment text;
    next_dir bigint;
    next_file bigint;
    next_symlink bigint;
    symlink_target_ symlink_pathname;
BEGIN
    FOR segment IN SELECT regexp_split_to_table(path, '/') LOOP
        IF (SELECT ino FROM dirs WHERE ino = current_ino) IS NULL THEN
            RAISE EXCEPTION 'inode % is not a directory', current_ino;
        END IF;

        -- Treat "" as a no-op to handle /some//path and /some/path/
        IF segment = '' THEN
            CONTINUE;
        END IF;

        IF segment = '.' THEN
            CONTINUE;
        END IF;

        IF segment = '..' THEN
            next_ino := (SELECT parent FROM dirents WHERE child_dir = current_ino);
            IF next_ino IS NULL THEN
                RAISE EXCEPTION 'inode % does not have a parent', current_ino;
            END IF;
            current_ino := next_ino;           
            CONTINUE;
        END IF;

        SELECT child_dir, child_file, child_symlink INTO next_dir, next_file, next_symlink FROM dirents WHERE parent = current_ino AND basename = segment);
        IF NOT FOUND THEN
            RAISE EXCEPTION 'inode % does not have dirent for %', current_ino, quote_literal(segment);
        END IF;

        IF next_symlink IS NOT NULL THEN
            symlink_target_ := (SELECT symlink_target FROM symlinks WHERE ino = next_ino);
            IF symlink_resolutions_left - 1 = 0 THEN
                RAISE EXCEPTION 'Too many levels of symbolic links';
            END IF;
            next_ino := (SELECT __get_ino_for_path(current_ino, symlink_target_, symlink_resolutions_left - 1));
        END IF;

        current_ino := next_ino;
    END LOOP;
    RETURN current_ino;
END;
$$ LANGUAGE plpgsql;

-- Look up inode by relative path, returning the type and ino.
--
-- Absolute path (i.e. one starting with '/') is not supported because the
-- database does not know which DIR inode was used for root DIR.
CREATE OR REPLACE FUNCTION get_ino_for_path(current_ino bigint, path text) RETURNS bigint AS $$
BEGIN
    -- Match the Linux behavior: "once the 40th symlink is detected,
    -- an error is returned" https://lwn.net/Articles/650786/
    RETURN __get_ino_for_path(current_ino, path, 40);
END;
$$ LANGUAGE plpgsql;

-- List a directory by path
CREATE OR REPLACE FUNCTION get_children_for_path(current_ino bigint, path text) RETURNS TABLE(basename linux_basename, child bigint) AS $$
DECLARE
    ino_ bigint;
BEGIN
    ino_ := get_ino_for_path(current_ino, path);
    IF (SELECT ino FROM dirs WHERE ino = ino_) IS NULL THEN
        RAISE EXCEPTION 'inode % is not a directory', ino_;
    END IF;
    RETURN QUERY SELECT dirents.basename, dirents.child FROM dirents WHERE parent = ino_;
END;
$$ LANGUAGE plpgsql;
