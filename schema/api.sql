CREATE PROCEDURE create_root_inode(hostname text, exastash_version integer)
LANGUAGE SQL
AS $$
    INSERT INTO dirs (ino, mtime, birth_time, birth_hostname, birth_version)
        VALUES (2, now()::timespec64, now()::timespec64, hostname, exastash_version);
$$;

CREATE OR REPLACE FUNCTION __get_ino_for_path(current_ino bigint, path text, symlink_resolutions_left int) RETURNS bigint AS $$
DECLARE
    segment text;
    next_ino bigint;
    symlink_target_ symlink_pathname;
BEGIN
    -- The very beginning of a path is the only place you can specify
    -- that you want the root directory.
    IF starts_with(path, '/') THEN
        current_ino := 2;
        path := (SELECT substr(path, 2));
    END IF;

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

        -- IF segment = '..' THEN
        --     current_ino := (SELECT parent_ino FROM inodes WHERE ino = current_ino);
        --     CONTINUE;
        -- END IF;

        next_ino := (SELECT child FROM dirents WHERE parent = current_ino AND basename = segment);
        IF next_ino IS NULL THEN
            RAISE EXCEPTION 'inode % does not have dirent for %', current_ino, quote_literal(segment);
        END IF;
        
        -- SELECT type, symlink_target INTO type_, symlink_target_ FROM inodes WHERE ino = next_ino;
        -- IF type_ = 'LNK' THEN
        --     IF symlink_resolutions_left - 1 = 0 THEN
        --         RAISE EXCEPTION 'Too many levels of symbolic links';
        --     END IF;
        --     next_ino := (SELECT __get_ino_for_path(current_ino, symlink_target_, symlink_resolutions_left - 1));
        -- END IF;

        current_ino := next_ino;
    END LOOP;
    RETURN current_ino;
END;
$$ LANGUAGE plpgsql;

-- Look up inode by relative or absolute path, returning just the ino
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
