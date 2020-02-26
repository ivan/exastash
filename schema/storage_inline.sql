CREATE TABLE storage_inline (
    file_id  bigint PRIMARY KEY REFERENCES files (id),
    -- check length > 0 because there is no need to store empty content
    content  bytea  NOT NULL CHECK (length(content) > 0)
);

CREATE TRIGGER storage_inline_check_update
    BEFORE UPDATE ON storage_inline
    FOR EACH ROW
    WHEN (OLD.file_id != NEW.file_id)
    EXECUTE FUNCTION raise_exception('cannot change file_id');

CREATE TRIGGER storage_inline_forbid_truncate
    BEFORE TRUNCATE ON storage_inline
    EXECUTE FUNCTION raise_exception('truncate is forbidden');
