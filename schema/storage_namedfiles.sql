CREATE TABLE storage_namedfiles (
    file_id      bigint       NOT NULL REFERENCES files,
    -- some identifying string e.g. "ceph cluster" also used elsewhere in policy.js
    location     text         NOT NULL,
    -- path with slashes
    pathname     text         NOT NULL CHECK (octet_length(pathname) >= 1),
    last_probed  timestamptz,

    -- We may know of more than one item that has the file.
    PRIMARY KEY (file_id, location)
);

CREATE TRIGGER storage_namedfiles_check_update
    BEFORE UPDATE ON storage_namedfiles
    FOR EACH ROW
    WHEN (
        OLD.file_id  != NEW.file_id  OR
        OLD.location != NEW.location
    )
    EXECUTE FUNCTION raise_exception('cannot change file_id or location');

CREATE TRIGGER storage_namedfiles_forbid_truncate
    BEFORE TRUNCATE ON storage_namedfiles
    EXECUTE FUNCTION raise_exception('truncate is forbidden');

-- Set the index to use for future CLUSTER operations
ALTER TABLE storage_namedfiles CLUSTER ON storage_namedfiles_pkey;
