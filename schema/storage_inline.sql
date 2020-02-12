CREATE TABLE storage_inline (
    file_id  bigint PRIMARY KEY REFERENCES files (id),
    content  bytea  NOT NULL
);
REVOKE TRUNCATE ON storage_inline FROM current_user;

CREATE TRIGGER storage_inline_check_update
    BEFORE UPDATE ON storage_inline
    FOR EACH ROW
    WHEN (OLD.file_id != NEW.file_id)
    EXECUTE FUNCTION raise_exception('cannot change file_id');
