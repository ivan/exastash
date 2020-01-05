CREATE TABLE storage_inline (
    ino      bigint PRIMARY KEY REFERENCES files,
    content  bytea  NOT NULL
);
REVOKE TRUNCATE ON storage_inline FROM current_user;

CREATE TRIGGER storage_inline_check_update
    BEFORE UPDATE ON storage_inline
    FOR EACH ROW
    WHEN (OLD.ino != NEW.ino)
    EXECUTE FUNCTION raise_exception('cannot change ino');
