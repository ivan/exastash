\set ON_ERROR_STOP on

CREATE SCHEMA stash;
SET search_path TO stash;

CREATE OR REPLACE FUNCTION raise_exception() RETURNS trigger AS $$
DECLARE
    message text;
BEGIN
    message := TG_ARGV[0];
    RAISE EXCEPTION '%', message;
END;
$$ LANGUAGE plpgsql;

-- A superuser should apply extensions.sql first

\ir exastash_versions.sql
\ir inodes.sql
-- If you add a new storage, remember to also add the table name to
-- file_ids_with_storage_or_zero_size and
-- file_ids_with_storage_or_zero_size_with_duplicates
\ir storage_fofs.sql
\ir storage_inline.sql
\ir storage_gdrive.sql
\ir storage_internetarchive.sql
\ir google_auth.sql
\ir dirents.sql
\ir inodes_views.sql
