CREATE TABLE storage_inline (
    ino      ino    PRIMARY KEY REFERENCES inodes,
    content  bytea  NOT NULL
);
REVOKE TRUNCATE ON storage_inline FROM current_user;

CREATE TRIGGER storage_inline_check_ino
    BEFORE INSERT ON storage_inline
    FOR EACH ROW
    EXECUTE FUNCTION assert_inode_is_regular_file();
