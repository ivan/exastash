BEGIN;

SELECT plan(2);

CALL create_root_inode('fake', 41);

-- Errors

-- ino 2 is the root directory
PREPARE cannot_add_content_for_dir AS INSERT INTO storage_inline (ino, content) VALUES (2, E'hello\nworld');
SELECT throws_ilike('cannot_add_content_for_dir', '%inode 2 is DIR, not a regular file%');

-- Successes

INSERT INTO inodes (ino, type, size, mtime, executable, birth_time, birth_hostname, birth_exastash_version)
    VALUES (100, 'REG', 11, now()::timespec64, false, now()::timespec64, 'fake', 41);

PREPARE insert_content AS INSERT INTO storage_inline (ino, content) VALUES (100, E'hello\nworld');
SELECT lives_ok('insert_content');

--

SELECT * FROM finish();

ROLLBACK;
