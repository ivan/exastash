CREATE DOMAIN pathname AS text
    CHECK (octet_length(VALUE) >= 1);

CREATE TABLE storage_namedfiles (
    file_id      bigint       NOT NULL REFERENCES files,
    -- some identifying string e.g. "ceph cluster" also used elsewhere in policy.js
    location     text         NOT NULL,
    pathname     pathname     NOT NULL,
    last_probed  timestamptz,

    -- We may know of more than one item that has the file.
    PRIMARY KEY (file_id, location)
);

CREATE TRIGGER storage_namedfiles_check_update
    BEFORE UPDATE ON storage_namedfiles
    FOR EACH ROW
    WHEN (
        OLD.file_id  != NEW.file_id  OR
        OLD.location != NEW.location OR
        OLD.pathname != NEW.pathname
    )
    EXECUTE FUNCTION raise_exception('cannot change file_id, location, or pathname');

CREATE TRIGGER storage_namedfiles_forbid_truncate
    BEFORE TRUNCATE ON storage_namedfiles
    EXECUTE FUNCTION raise_exception('truncate is forbidden');

-- Set the index to use for future CLUSTER operations
ALTER TABLE storage_namedfiles CLUSTER ON storage_namedfiles_pkey;
