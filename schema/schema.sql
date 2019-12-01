\set ON_ERROR_STOP on

CREATE OR REPLACE FUNCTION raise_exception() RETURNS trigger AS $$
DECLARE
    message text;
BEGIN
    message := TG_ARGV[0];
    RAISE EXCEPTION '%', message;
END;
$$ LANGUAGE plpgsql;

\ir exastash_versions.sql
\ir timespec64.sql
\ir inodes.sql
\ir storage_inline.sql
\ir storage_gdrive.sql
\ir storage_internetarchive.sql
\ir dirents.sql
