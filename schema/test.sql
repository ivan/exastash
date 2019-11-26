BEGIN;

SELECT plan(4);
SELECT has_table('inodes');

PREPARE insert_reg_with_inline_content AS INSERT INTO inodes (
  ino, type, size, mtime, executable, inline_content, symlink_target
) VALUES (0, 'REG', 5, (0, 0), false, 'hello', NULL);
SELECT lives_ok('insert_reg_with_inline_content', 'can insert a regular file with inline content');

PREPARE insert_lnk AS INSERT INTO inodes (
  ino, type, size, mtime, executable, inline_content, symlink_target
) VALUES (1, 'LNK', NULL, (0, 0), NULL, NULL, '../some/target');
SELECT lives_ok('insert_lnk', 'can insert a symbolic link');

PREPARE insert_dir AS INSERT INTO inodes (
  ino, type, size, mtime, executable, inline_content, symlink_target
) VALUES (2, 'DIR', NULL, (0, 0), NULL, NULL, NULL);
SELECT lives_ok('insert_dir', 'can insert a directory');

SELECT * FROM finish();

ROLLBACK;
