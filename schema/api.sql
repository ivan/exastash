-- Look up inode by relative or absolute path, returning just the ino
CREATE OR REPLACE FUNCTION get_ino_for_path(current_ino bigint, path text) RETURNS ino AS $$
DECLARE
    segment text;
    next_ino bigint;
    type_ inode_type;
    symlink_target_ symlink_pathname;
BEGIN
    -- The very beginning of a path is the only place you can specify
    -- that you want the root directory.
    IF starts_with(path, '/') THEN
        current_ino := 2;
        path := (SELECT substr(path, 2));
    END IF;

    FOR segment IN SELECT regexp_split_to_table(path, '/') LOOP
        -- If we're a directory and segment is "", treat that as a no-op
        -- to handle /some//path and /some/path/
        IF segment = '' AND (SELECT type FROM inodes WHERE ino = current_ino) = 'DIR' THEN
            CONTINUE;
        END IF;

        IF segment = '.' THEN
            CONTINUE;
        END IF;

        IF segment = '..' THEN
            current_ino := (SELECT parent_ino FROM inodes WHERE ino = current_ino);
            CONTINUE;
        END IF;

        next_ino := (SELECT child FROM dirents WHERE parent = current_ino AND basename = segment);
        IF next_ino IS NULL THEN
            RAISE EXCEPTION 'inode % does not have dirent for %', current_ino, quote_literal(segment);
        END IF;
        current_ino := next_ino;

        SELECT type, symlink_target INTO type_, symlink_target_ FROM inodes WHERE ino = current_ino;
        IF next_type = 'LNK' THEN
            current_ino := (SELECT get_ino_for_path(current_ino, symlink_target_));
        END IF;
    END LOOP;
    RETURN current_ino;
END;
$$ LANGUAGE plpgsql;
