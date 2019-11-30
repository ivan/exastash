BEGIN;

SELECT plan(2);

-- Errors

-- ino 2 is the root directory
PREPARE cannot_add_content_for_dir AS INSERT INTO storage_inline (ino, content) VALUES (2, E'hello\nworld');
SELECT throws_ilike('cannot_add_content_for_dir', '%inode 2 is DIR, not a regular file%');

-- Successes

INSERT INTO inodes (ino, type, size, mtime, executable) VALUES (100, 'REG', 11, now()::timespec64, false);

PREPARE insert_content AS INSERT INTO storage_inline (ino, content) VALUES (100, E'hello\nworld');
SELECT lives_ok('insert_content');

--

SELECT * FROM finish();

ROLLBACK;
