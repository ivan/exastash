CREATE DOMAIN gdrive_domain AS text
    CHECK (length(VALUE) >= 1 AND length(VALUE) <= 255);

CREATE TABLE gdrive_domains (
    gdrive_domain  gdrive_domain  PRIMARY KEY
    -- TODO: access keys
);
REVOKE TRUNCATE ON gdrive_domains FROM current_user;

CREATE DOMAIN md5     AS bytea CHECK (length(VALUE) = 16);
CREATE DOMAIN crc32c  AS bytea CHECK (length(VALUE) = 4);
-- the shortest file_id we have is 28
-- the longest file_id we have is 33, but allow up to 160 in case Google changes format
CREATE DOMAIN file_id AS text  CHECK (VALUE ~ '\A[-_0-9A-Za-z]{28,160}\Z');

CREATE TABLE gdrive_files (
    file_id         file_id      PRIMARY KEY,
    -- forbid very long owner names
    -- some of our old chunks have no recorded owner
    file_owner      text         CHECK (file_owner ~ '\A.{1,255}\Z'),
    md5             md5          NOT NULL,
    crc32c          crc32c       NOT NULL,
    size            bigint       NOT NULL CHECK (size >= 1),
    last_probed     timestamptz
);
REVOKE TRUNCATE ON gdrive_files FROM current_user;

CREATE TRIGGER gdrive_files_check_update
    BEFORE UPDATE ON gdrive_files
    FOR EACH ROW
    WHEN (
        OLD.file_id != NEW.file_id OR
        OLD.md5 != NEW.md5 OR
        OLD.crc32c != NEW.crc32c OR
        OLD.size != NEW.size
    )
    EXECUTE FUNCTION raise_exception('cannot change file_id, md5, crc32c, or size');

CREATE OR REPLACE FUNCTION gdrive_files_not_referenced() RETURNS trigger AS $$
DECLARE
    chunk_sequence bigint;
BEGIN
    chunk_sequence := (SELECT chunk_sequence FROM gdrive_chunk_sequences WHERE files @> OLD.file_id LIMIT 1);
    IF chunk_sequence IS NOT NULL THEN
        RAISE EXCEPTION 'file_id still referenced by chunk_sequence=%', chunk_sequence;
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER gdrive_files_check_delete
    BEFORE DELETE ON gdrive_files
    FOR EACH ROW
    EXECUTE FUNCTION gdrive_files_not_referenced();

CREATE TABLE gdrive_chunk_sequences (
    chunk_sequence  bigint     GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY CHECK (chunk_sequence >= 1),
    -- ordered list of files
    files           file_id[]  NOT NULL CHECK (cardinality(files) >= 1)
);
REVOKE TRUNCATE ON gdrive_chunk_sequences FROM current_user;

CREATE INDEX file_id_index ON gdrive_chunk_sequences USING GIN (files);

CREATE OR REPLACE FUNCTION assert_files_exist_in_gdrive_files() RETURNS trigger AS $$
DECLARE
    file_count integer;
BEGIN
    -- This catches not only missing files but also duplicate entries in NEW.files
    file_count := (SELECT COUNT(file_id) FROM gdrive_files WHERE file_id IN (SELECT unnest(NEW.files)));
    IF file_count != cardinality(NEW.files) THEN
        RAISE EXCEPTION 'chunk sequence had % files: % but only % of these are in gdrive_files',
            cardinality(NEW.files), NEW.files, file_count;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER gdrive_chunk_sequences_check_files
    BEFORE INSERT ON gdrive_chunk_sequences
    FOR EACH ROW
    EXECUTE FUNCTION assert_files_exist_in_gdrive_files();

CREATE TRIGGER gdrive_chunk_sequences_check_update
    BEFORE UPDATE ON gdrive_chunk_sequences
    FOR EACH ROW
    EXECUTE FUNCTION raise_exception('cannot change chunk_sequence or files');

CREATE TABLE storage_gdrive (
    ino             bigint         NOT NULL REFERENCES inodes,
    gdrive_domain   gdrive_domain  NOT NULL REFERENCES gdrive_domains,
    chunk_sequence  bigint         NOT NULL REFERENCES gdrive_chunk_sequences,

    -- Include chunk_sequence in the key because we might want to reupload
    -- some chunk sequences in a new format.
    PRIMARY KEY (ino, gdrive_domain, chunk_sequence)
);
REVOKE TRUNCATE ON storage_gdrive FROM current_user;

CREATE TRIGGER storage_gdrive_check_ino
    BEFORE INSERT ON storage_gdrive
    FOR EACH ROW
    EXECUTE FUNCTION assert_inode_is_regular_file();

CREATE TRIGGER storage_gdrive_check_update
    BEFORE UPDATE ON storage_gdrive
    FOR EACH ROW
    EXECUTE FUNCTION raise_exception('cannot change ino, gdrive_domain, or chunk_sequence');
