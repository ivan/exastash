CREATE DOMAIN ia_item AS text
    CHECK (
        -- https://help.archive.org/hc/en-us/articles/360018818271-Internet-Archive-Metadata
        VALUE ~ '\A[A-Za-z0-9][-_\.A-Za-z0-9]{0,99}\Z'
    );

CREATE DOMAIN ia_pathname AS text
    CHECK (
        octet_length(VALUE) >= 1 AND
        octet_length(VALUE) <= 1024 -- the true maximum is unknown
    );

CREATE TABLE storage_internetarchive (
    file_id       bigint       NOT NULL REFERENCES files,
    ia_item       ia_item      NOT NULL,
    pathname      ia_pathname  NOT NULL,
    darked        boolean      NOT NULL DEFAULT false,
    last_probed   timestamptz,

    -- We may know of more than one item that has the file.
    PRIMARY KEY (file_id, ia_item)
);
REVOKE TRUNCATE ON storage_internetarchive FROM current_user;

CREATE TRIGGER storage_internetarchive_check_update
    BEFORE UPDATE ON storage_internetarchive
    FOR EACH ROW
    WHEN (
        OLD.file_id  != NEW.file_id OR
        OLD.ia_item  != NEW.ia_item OR
        OLD.pathname != NEW.pathname
    )
    EXECUTE FUNCTION raise_exception('cannot change file_id, ia_item, or pathname');
