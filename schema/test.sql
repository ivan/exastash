BEGIN;

SELECT plan(18);
SELECT has_table('inodes');

PREPARE cannot_insert_with_negative_ino AS INSERT INTO inodes (
  ino, type, size, mtime, executable, inline_content, symlink_target
) VALUES (-1, 'REG', 5, (0, 0), false, 'hello', NULL);
SELECT throws_ilike('cannot_insert_with_negative_ino', '%violates check constraint%');

PREPARE cannot_insert_with_zero_ino AS INSERT INTO inodes (
  ino, type, size, mtime, executable, inline_content, symlink_target
) VALUES (0, 'REG', 5, (0, 0), false, 'hello', NULL);
SELECT throws_ilike('cannot_insert_with_zero_ino', '%violates check constraint%');

PREPARE cannot_insert_with_one_ino AS INSERT INTO inodes (
  ino, type, size, mtime, executable, inline_content, symlink_target
) VALUES (1, 'REG', 5, (0, 0), false, 'hello', NULL);
SELECT throws_ilike('cannot_insert_with_zero_ino', '%violates check constraint%');

PREPARE cannot_insert_dir_with_size AS INSERT INTO inodes (
  type, size, mtime, executable, inline_content, symlink_target
) VALUES ('DIR', 0, (0, 0), NULL, NULL, NULL);
SELECT throws_ilike('cannot_insert_dir_with_size', '%violates check constraint%');

PREPARE cannot_insert_lnk_with_size AS INSERT INTO inodes (
  type, size, mtime, executable, inline_content, symlink_target
) VALUES ('LNK', 0, (0, 0), NULL, NULL, NULL);
SELECT throws_ilike('cannot_insert_lnk_with_size', '%violates check constraint%');

PREPARE cannot_insert_dir_with_executable AS INSERT INTO inodes (
  type, size, mtime, executable, inline_content, symlink_target
) VALUES ('DIR', NULL, (0, 0), true, NULL, NULL);
SELECT throws_ilike('cannot_insert_dir_with_executable', '%violates check constraint%');

PREPARE cannot_insert_lnk_with_executable AS INSERT INTO inodes (
  type, size, mtime, executable, inline_content, symlink_target
) VALUES ('LNK', NULL, (0, 0), true, NULL, NULL);
SELECT throws_ilike('cannot_insert_lnk_with_executable', '%violates check constraint%');

PREPARE cannot_insert_dir_with_inline_content AS INSERT INTO inodes (
  type, size, mtime, executable, inline_content, symlink_target
) VALUES ('DIR', NULL, (0, 0), NULL, '', NULL);
SELECT throws_ilike('cannot_insert_dir_with_inline_content', '%violates check constraint%');

PREPARE cannot_insert_lnk_with_inline_content AS INSERT INTO inodes (
  type, size, mtime, executable, inline_content, symlink_target
) VALUES ('LNK', NULL, (0, 0), NULL, '', NULL);
SELECT throws_ilike('cannot_insert_lnk_with_inline_content', '%violates check constraint%');

PREPARE cannot_insert_reg_with_symlink_target AS INSERT INTO inodes (
  type, size, mtime, executable, inline_content, symlink_target
) VALUES ('LNK', 0, (0, 0), true, '', '../some/target');
SELECT throws_ilike('cannot_insert_reg_with_symlink_target', '%violates check constraint%');

PREPARE cannot_insert_dir_with_symlink_target AS INSERT INTO inodes (
  type, size, mtime, executable, inline_content, symlink_target
) VALUES ('DIR', NULL, (0, 0), NULL, NULL, '../some/target');
SELECT throws_ilike('cannot_insert_dir_with_symlink_target', '%violates check constraint%');

PREPARE cannot_insert_reg_with_wrong_size AS INSERT INTO inodes (
  type, size, mtime, executable, inline_content, symlink_target
) VALUES ('REG', 6, (0, 0), true, 'hello', NULL);
SELECT throws_ilike('cannot_insert_reg_with_wrong_size', '%violates check constraint%');

PREPARE insert_reg_with_inline_content AS INSERT INTO inodes (
  type, size, mtime, executable, inline_content, symlink_target
) VALUES ('REG', 5, (0, 0), false, 'hello', NULL);
SELECT lives_ok('insert_reg_with_inline_content');

PREPARE insert_lnk AS INSERT INTO inodes (
  type, size, mtime, executable, inline_content, symlink_target
) VALUES ('LNK', NULL, (0, 0), NULL, NULL, '../some/target');
SELECT lives_ok('insert_lnk');

PREPARE insert_lnk_with_target_1024_bytes AS INSERT INTO inodes (
  type, size, mtime, executable, inline_content, symlink_target
) VALUES ('LNK', NULL, (0, 0), NULL, NULL, repeat('x', 1024));
SELECT lives_ok('insert_lnk_with_target_1024_bytes');

PREPARE cannot_insert_lnk_with_target_over_1024_bytes AS INSERT INTO inodes (
  type, size, mtime, executable, inline_content, symlink_target
) VALUES ('LNK', NULL, (0, 0), NULL, NULL, repeat('x', 1025));
SELECT throws_ilike('cannot_insert_lnk_with_target_over_1024_bytes', '%violates check constraint%');

PREPARE insert_dir AS INSERT INTO inodes (
  type, size, mtime, executable, inline_content, symlink_target
) VALUES ('DIR', NULL, (0, 0), NULL, NULL, NULL);
SELECT lives_ok('insert_dir');

SELECT * FROM finish();

ROLLBACK;
