-- Look up inode by relative or absolute path, returns just the ino
-- If relative path is given, the "current" directory is set to current_ino
CREATE OR REPLACE FUNCTION get_ino_for_path(current_ino bigint, path text) RETURNS ino AS $$
DECLARE
    segment linux_basename;
    next_ino bigint;
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
        IF segment = '' THEN
            CONTINUE;
        END IF;

        next_ino := (SELECT child FROM dirents WHERE parent = current_ino AND basename = segment);
        IF next_ino IS NULL THEN
            RAISE EXCEPTION 'inode % does not have dirent for %', current_ino, quote_literal(segment);
        END IF;
        current_ino := next_ino;
    END LOOP;
    RETURN next_ino;
END;
$$ LANGUAGE plpgsql;
