-- Owners of Google Drive files

CREATE TABLE gdrive_owners (
    id     int   GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY CHECK (id >= 1),
    -- email or other identifying string
    owner  text  CHECK (owner ~ '\A.{1,255}\Z')
);

CREATE UNIQUE INDEX gdrive_owners_owner_index ON gdrive_owners (owner);

CREATE TRIGGER gdrive_owners_check_update
    BEFORE UPDATE ON gdrive_owners
    FOR EACH ROW
    WHEN (OLD.id != NEW.id)
    EXECUTE FUNCTION raise_exception('cannot change id');

CREATE TRIGGER gdrive_owners_forbid_truncate
    BEFORE TRUNCATE ON gdrive_owners
    EXECUTE FUNCTION raise_exception('truncate is forbidden');



-- Google Drive files

-- Columns are ordered for optimal packing, be careful
CREATE TABLE gdrive_files (
    -- Not a UUID, just using uuid as a 128-bit field instead of bytea to save one byte
    md5          uuid         NOT NULL,
    size         bigint       NOT NULL CHECK (size >= 1),
    last_probed  timestamptz,
    -- crc32c is actually unsigned, but use an int instead of bytea to save one byte
    crc32c       int          NOT NULL,
    -- Can be NULL because some of our old chunks have no recorded owner
    owner        int          REFERENCES gdrive_owners,
    -- The shortest gdrive_id we have is 28
    -- The longest gdrive_id we have is 33, but allow up to 160 in case Google changes format
    id           text         PRIMARY KEY CHECK (id ~ '\A[-_0-9A-Za-z]{28,160}\Z')
);

CREATE TRIGGER gdrive_files_check_update
    BEFORE UPDATE ON gdrive_files
    FOR EACH ROW
    WHEN (
        OLD.id     != NEW.id     OR
        OLD.md5    != NEW.md5    OR
        OLD.crc32c != NEW.crc32c OR
        OLD.size   != NEW.size
    )
    EXECUTE FUNCTION raise_exception('cannot change id, md5, crc32c, or size');

CREATE OR REPLACE FUNCTION gdrive_files_not_referenced() RETURNS trigger AS $$
DECLARE
    file_id_ bigint;
BEGIN
    -- TODO: make sure index is actually being used for this
    file_id_ := (SELECT file_id FROM stash.storage_gdrive WHERE gdrive_ids @> ARRAY[OLD.id] LIMIT 1);
    IF file_id_ IS NOT NULL THEN
        RAISE EXCEPTION 'gdrive_files=% is still referenced by storage_gdrive=%', OLD.id, file_id_;
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER gdrive_files_check_delete
    BEFORE DELETE ON gdrive_files
    FOR EACH ROW
    EXECUTE FUNCTION gdrive_files_not_referenced();

CREATE TRIGGER gdrive_files_forbid_truncate
    BEFORE TRUNCATE ON gdrive_files
    EXECUTE FUNCTION raise_exception('truncate is forbidden');



-- G Suite domains

CREATE TABLE gsuite_domains (
    id      smallint  GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY CHECK (id >= 1),
    domain  text      CHECK (domain ~ '\A.{1,255}\Z')
    -- TODO: access keys
);

CREATE UNIQUE INDEX gsuite_domains_domain_index ON gsuite_domains (domain);

CREATE TRIGGER gsuite_domains_check_update
    BEFORE UPDATE ON gsuite_domains
    FOR EACH ROW
    WHEN (OLD.id != NEW.id)
    EXECUTE FUNCTION raise_exception('cannot change id');

CREATE TRIGGER gsuite_domains_forbid_truncate
    BEFORE TRUNCATE ON gsuite_domains
    EXECUTE FUNCTION raise_exception('truncate is forbidden');



-- Storage (sequences of Google Drive files)

CREATE TYPE cipher AS ENUM ('AES_128_CTR', 'AES_128_GCM');

-- Columns are ordered for optimal packing, be careful
CREATE TABLE storage_gdrive (
    -- Not a UUID, just using uuid as a 128-bit field instead of bytea to save one byte
    cipher_key     uuid      NOT NULL,
    file_id        bigint    NOT NULL REFERENCES files (id),
    cipher         cipher    NOT NULL,
    gsuite_domain  smallint  NOT NULL REFERENCES gsuite_domains (id),
    -- An sequence of encrypted chunks stored in Google Drive
    --
    -- Imagine a REFERENCES on on gdrive_files (id) here; PostgreSQL 12 doesn't
    -- support it for array elements, so we have two triggers to emulate it.
    --
    -- Don't use an array of DOMAIN type here to avoid confusing rust-postgres
    gdrive_ids     text[]    NOT NULL CHECK (cardinality(gdrive_ids) >= 1),

    -- We don't need more than one of these per this triple.
    PRIMARY KEY (file_id, gsuite_domain, cipher)
);

CREATE INDEX gdrive_gdrive_ids_index ON storage_gdrive USING GIN (gdrive_ids);

CREATE OR REPLACE FUNCTION assert_files_exist_in_gdrive_files() RETURNS trigger AS $$
DECLARE
    ids text[];
    file_count integer;
BEGIN
    -- Use FOR KEY SHARE to prevent another concurrent transaction from deleting the
    -- gdrive files we're referencing from gdrive_ids.
    ids := ARRAY(SELECT id FROM stash.gdrive_files WHERE id IN (SELECT unnest(NEW.gdrive_ids)) FOR KEY SHARE);
    -- This catches not only missing gdrive_ids but also duplicate entries in NEW.gdrive_ids
    file_count := cardinality(ids);
    IF file_count != cardinality(NEW.gdrive_ids) THEN
        RAISE EXCEPTION 'gdrive_ids had % ids: % but only % of these are in gdrive_files',
            cardinality(NEW.gdrive_ids), NEW.gdrive_ids, file_count;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER storage_gdrive_check_files
    BEFORE INSERT ON storage_gdrive
    FOR EACH ROW
    EXECUTE FUNCTION assert_files_exist_in_gdrive_files();

CREATE TRIGGER storage_gdrive_check_update
    BEFORE UPDATE ON storage_gdrive
    FOR EACH ROW
    EXECUTE FUNCTION raise_exception('cannot change file_id, gsuite_domain, cipher, cipher_key, or gdrive_ids');

CREATE TRIGGER storage_gdrive_forbid_truncate
    BEFORE TRUNCATE ON storage_gdrive
    EXECUTE FUNCTION raise_exception('truncate is forbidden');
