BEGIN;

SELECT plan(15);

-- CHECK constraints

PREPARE cannot_insert_with_negative_ino AS INSERT INTO inodes (
  ino, type, size, mtime, executable, symlink_target
) VALUES (-1, 'REG', 5, (0, 0), false, NULL);
SELECT throws_ilike('cannot_insert_with_negative_ino', '%violates check constraint%');

PREPARE cannot_insert_with_zero_ino AS INSERT INTO inodes (
  ino, type, size, mtime, executable, symlink_target
) VALUES (0, 'REG', 5, (0, 0), false, NULL);
SELECT throws_ilike('cannot_insert_with_zero_ino', '%violates check constraint%');

PREPARE cannot_insert_with_one_ino AS INSERT INTO inodes (
  ino, type, size, mtime, executable, symlink_target
) VALUES (1, 'REG', 5, (0, 0), false, NULL);
SELECT throws_ilike('cannot_insert_with_one_ino', '%violates check constraint%');

PREPARE cannot_insert_dir_with_size AS INSERT INTO inodes (
  type, size, mtime, executable, symlink_target
) VALUES ('DIR', 0, (0, 0), NULL, NULL);
SELECT throws_ilike('cannot_insert_dir_with_size', '%violates check constraint%');

PREPARE cannot_insert_lnk_with_size AS INSERT INTO inodes (
  type, size, mtime, executable, symlink_target
) VALUES ('LNK', 0, (0, 0), NULL, NULL);
SELECT throws_ilike('cannot_insert_lnk_with_size', '%violates check constraint%');

PREPARE cannot_insert_dir_with_executable AS INSERT INTO inodes (
  type, size, mtime, executable, symlink_target
) VALUES ('DIR', NULL, (0, 0), true, NULL);
SELECT throws_ilike('cannot_insert_dir_with_executable', '%violates check constraint%');

PREPARE cannot_insert_lnk_with_executable AS INSERT INTO inodes (
  type, size, mtime, executable, symlink_target
) VALUES ('LNK', NULL, (0, 0), true, NULL);
SELECT throws_ilike('cannot_insert_lnk_with_executable', '%violates check constraint%');

PREPARE cannot_insert_reg_with_symlink_target AS INSERT INTO inodes (
  type, size, mtime, executable, symlink_target
) VALUES ('LNK', 0, (0, 0), true, '../some/target');
SELECT throws_ilike('cannot_insert_reg_with_symlink_target', '%violates check constraint%');

PREPARE cannot_insert_dir_with_symlink_target AS INSERT INTO inodes (
  type, size, mtime, executable, symlink_target
) VALUES ('DIR', NULL, (0, 0), NULL, '../some/target');
SELECT throws_ilike('cannot_insert_dir_with_symlink_target', '%violates check constraint%');

PREPARE cannot_insert_lnk_with_target_over_1024_bytes AS INSERT INTO inodes (
  type, size, mtime, executable, symlink_target
) VALUES ('LNK', NULL, (0, 0), NULL, repeat('x', 1025));
SELECT throws_ilike('cannot_insert_lnk_with_target_over_1024_bytes', '%violates check constraint%');

PREPARE cannot_delete_root_inode AS DELETE FROM inodes WHERE ino = 2;
SELECT throws_ilike('cannot_delete_root_inode', '%cannot delete%');

-- Successes

PREPARE insert_reg AS INSERT INTO inodes (
  type, size, mtime, executable
) VALUES ('REG', 20, (0, 0), true);
SELECT lives_ok('insert_reg');

PREPARE insert_dir AS INSERT INTO inodes (
  type, size, mtime, parent_ino
) VALUES ('DIR', NULL, (0, 0), 2);
SELECT lives_ok('insert_dir');

PREPARE insert_lnk AS INSERT INTO inodes (
  type, size, mtime, executable, symlink_target
) VALUES ('LNK', NULL, (0, 0), NULL, '../some/target');
SELECT lives_ok('insert_lnk');

PREPARE insert_lnk_with_target_1024_bytes AS INSERT INTO inodes (
  type, size, mtime, symlink_target
) VALUES ('LNK', NULL, (0, 0), repeat('x', 1024));
SELECT lives_ok('insert_lnk_with_target_1024_bytes');

--

SELECT * FROM finish();

ROLLBACK;
