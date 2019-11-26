BEGIN;

SELECT plan(5);
SELECT has_table('inodes');

PREPARE insert_reg_with_inline_content AS INSERT INTO inodes (
  ino, type, size, mtime, executable, inline_content, symlink_target
) VALUES (0, 'REG', 5, (0, 0), false, 'hello', NULL);
SELECT lives_ok('insert_reg_with_inline_content');

PREPARE insert_lnk AS INSERT INTO inodes (
  ino, type, size, mtime, executable, inline_content, symlink_target
) VALUES (1, 'LNK', NULL, (0, 0), NULL, NULL, '../some/target');
SELECT lives_ok('insert_lnk');

PREPARE insert_dir AS INSERT INTO inodes (
  ino, type, size, mtime, executable, inline_content, symlink_target
) VALUES (2, 'DIR', NULL, (0, 0), NULL, NULL, NULL);
SELECT lives_ok('insert_dir');

PREPARE cannot_insert_with_negative_ino AS INSERT INTO inodes (
  ino, type, size, mtime, executable, inline_content, symlink_target
) VALUES (-1, 'REG', 5, (0, 0), false, 'hello', NULL);
SELECT throws_ilike('cannot_insert_with_negative_ino', '%violates check constraint%');

SELECT * FROM finish();

ROLLBACK;
