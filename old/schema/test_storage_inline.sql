BEGIN;

SELECT plan(3);

CALL create_root_inode('fake', 41);

-- ino 2 is the root directory
PREPARE cannot_add_content_for_dir AS INSERT INTO storage_inline (ino, content) VALUES (2, E'hello\nworld');
SELECT throws_like('cannot_add_content_for_dir', '%inode 2 is DIR, not a regular file%');

INSERT INTO files (ino, size, mtime, executable, birth_time, birth_hostname, birth_version)
    VALUES (1000, 11, now(), false, now(), 'fake', 41);

PREPARE insert_content AS INSERT INTO storage_inline (ino, content) VALUES (1000, E'hello\nworld');
SELECT lives_ok('insert_content');

PREPARE cannot_update AS UPDATE storage_inline SET ino = 2 WHERE ino = 1000;
SELECT throws_like('cannot_update', 'cannot change ino');

--

SELECT * FROM finish();

ROLLBACK;
