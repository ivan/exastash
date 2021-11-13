CREATE TABLE storage_inline (
    file_id       bigint  PRIMARY KEY REFERENCES files (id),
    -- We store zstd-compressed content because we want better compression ratios than PGLZ
    -- provides, and because we want compression for < 2KB files.  We cannot rely on btrfs
    -- compression because CoW interacts poorly with PostgreSQL under heavy write load,
    -- causing btrfs to run out of free space due to insufficiently aggressive GC.
    content_zstd  bytea   NOT NULL
);

-- EXTERNAL means TOAST but not compressed by PostgreSQL (content_zstd is already compressed)
ALTER TABLE storage_inline
    ALTER COLUMN content_zstd
    SET STORAGE EXTERNAL;

CREATE TRIGGER storage_inline_check_update
    BEFORE UPDATE ON storage_inline
    FOR EACH ROW
    WHEN (OLD.file_id != NEW.file_id)
    EXECUTE FUNCTION raise_exception('cannot change file_id');

CREATE TRIGGER storage_inline_forbid_truncate
    BEFORE TRUNCATE ON storage_inline
    EXECUTE FUNCTION raise_exception('truncate is forbidden');

-- Set the index to use for future CLUSTER operations
ALTER TABLE storage_inline CLUSTER ON storage_inline_pkey;
