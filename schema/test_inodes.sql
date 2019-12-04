BEGIN;

SELECT plan(18);

-- CHECK constraints

CALL create_root_inode('fake', 41);

PREPARE cannot_insert_with_negative_ino AS INSERT INTO inodes (
    ino, type, size, mtime, executable, symlink_target, birth_time, birth_hostname, birth_exastash_version
) VALUES (-1, 'REG', 5, (0, 0), false, NULL, (0, 0), 'fake', 41);
SELECT throws_like('cannot_insert_with_negative_ino', '%violates check constraint%');

PREPARE cannot_insert_with_zero_ino AS INSERT INTO inodes (
    ino, type, size, mtime, executable, symlink_target, birth_time, birth_hostname, birth_exastash_version
) VALUES (0, 'REG', 5, (0, 0), false, NULL, (0, 0), 'fake', 41);
SELECT throws_like('cannot_insert_with_zero_ino', '%violates check constraint%');

PREPARE cannot_insert_with_one_ino AS INSERT INTO inodes (
    ino, type, size, mtime, executable, symlink_target, birth_time, birth_hostname, birth_exastash_version
) VALUES (1, 'REG', 5, (0, 0), false, NULL, (0, 0), 'fake', 41);
SELECT throws_like('cannot_insert_with_one_ino', '%violates check constraint%');

PREPARE cannot_insert_dir_with_size AS INSERT INTO inodes (
    type, size, mtime, executable, symlink_target, birth_time, birth_hostname, birth_exastash_version
) VALUES ('DIR', 0, (0, 0), NULL, NULL, (0, 0), 'fake', 41);
SELECT throws_like('cannot_insert_dir_with_size', '%violates check constraint%');

PREPARE cannot_insert_lnk_with_size AS INSERT INTO inodes (
    type, size, mtime, executable, symlink_target, birth_time, birth_hostname, birth_exastash_version
) VALUES ('LNK', 0, (0, 0), NULL, NULL, (0, 0), 'fake', 41);
SELECT throws_like('cannot_insert_lnk_with_size', '%violates check constraint%');

PREPARE cannot_insert_dir_with_executable AS INSERT INTO inodes (
    type, size, mtime, executable, symlink_target, birth_time, birth_hostname, birth_exastash_version
) VALUES ('DIR', NULL, (0, 0), true, NULL, (0, 0), 'fake', 41);
SELECT throws_like('cannot_insert_dir_with_executable', '%violates check constraint%');

PREPARE cannot_insert_lnk_with_executable AS INSERT INTO inodes (
    type, size, mtime, executable, symlink_target, birth_time, birth_hostname, birth_exastash_version
) VALUES ('LNK', NULL, (0, 0), true, NULL, (0, 0), 'fake', 41);
SELECT throws_like('cannot_insert_lnk_with_executable', '%violates check constraint%');

PREPARE cannot_insert_reg_with_symlink_target AS INSERT INTO inodes (
    type, size, mtime, executable, symlink_target, birth_time, birth_hostname, birth_exastash_version
) VALUES ('LNK', 0, (0, 0), true, '../some/target', (0, 0), 'fake', 41);
SELECT throws_like('cannot_insert_reg_with_symlink_target', '%violates check constraint%');

PREPARE cannot_insert_dir_with_symlink_target AS INSERT INTO inodes (
    type, size, mtime, executable, symlink_target, birth_time, birth_hostname, birth_exastash_version
) VALUES ('DIR', NULL, (0, 0), NULL, '../some/target', (0, 0), 'fake', 41);
SELECT throws_like('cannot_insert_dir_with_symlink_target', '%violates check constraint%');

PREPARE cannot_insert_lnk_with_target_over_1024_bytes AS INSERT INTO inodes (
    type, size, mtime, executable, symlink_target, birth_time, birth_hostname, birth_exastash_version
) VALUES ('LNK', NULL, (0, 0), NULL, repeat('x', 1025), (0, 0), 'fake', 41);
SELECT throws_like('cannot_insert_lnk_with_target_over_1024_bytes', '%violates check constraint%');

PREPARE cannot_delete_root_inode AS DELETE FROM inodes WHERE ino = 2;
SELECT throws_like('cannot_delete_root_inode', '%cannot delete%');

PREPARE insert_reg AS INSERT INTO inodes (
    ino, type, size, mtime, executable, birth_time, birth_hostname, birth_exastash_version
) VALUES (90, 'REG', 20, (0, 0), true, (0, 0), 'fake', 41);
SELECT lives_ok('insert_reg');

PREPARE insert_dir AS INSERT INTO inodes (
    ino, type, size, mtime, parent_ino, birth_time, birth_hostname, birth_exastash_version
) VALUES (100, 'DIR', NULL, (0, 0), 2, (0, 0), 'fake', 41);
SELECT lives_ok('insert_dir');

-- Parent the dir
INSERT INTO dirents (parent, basename, child) VALUES (2, 'dir', 100);

PREPARE cannot_insert_dir_with_invalid_parent_ino AS INSERT INTO inodes (
    ino, type, size, mtime, parent_ino, birth_time, birth_hostname, birth_exastash_version
) VALUES (100, 'DIR', NULL, (0, 0), 9000, (0, 0), 'fake', 41);
SELECT throws_like('cannot_insert_dir_with_invalid_parent_ino', 'parent_ino=9000 does not exist');

PREPARE cannot_insert_inode_with_invalid_dirents_count AS INSERT INTO inodes (
    ino, type, size, mtime, parent_ino, birth_time, birth_hostname, birth_exastash_version, dirents_count
) VALUES (100, 'DIR', NULL, (0, 0), 2, (0, 0), 'fake', 41, 1);
SELECT throws_like('cannot_insert_inode_with_invalid_dirents_count', 'If given, dirents_count must be 0');

PREPARE cannot_insert_inode_with_invalid_child_dir_count AS INSERT INTO inodes (
    ino, type, size, mtime, parent_ino, birth_time, birth_hostname, birth_exastash_version, child_dir_count
) VALUES (100, 'DIR', NULL, (0, 0), 2, (0, 0), 'fake', 41, 1);
SELECT throws_like('cannot_insert_inode_with_invalid_child_dir_count', 'If given, child_dir_count must be 0 when inserting a DIR');

PREPARE insert_lnk AS INSERT INTO inodes (
    type, size, mtime, executable, symlink_target, birth_time, birth_hostname, birth_exastash_version
) VALUES ('LNK', NULL, (0, 0), NULL, '../some/target', (0, 0), 'fake', 41);
SELECT lives_ok('insert_lnk');

PREPARE insert_lnk_with_target_1024_bytes AS INSERT INTO inodes (
    type, size, mtime, symlink_target, birth_time, birth_hostname, birth_exastash_version
) VALUES ('LNK', NULL, (0, 0), repeat('x', 1024), (0, 0), 'fake', 41);
SELECT lives_ok('insert_lnk_with_target_1024_bytes');

--

SELECT * FROM finish();

ROLLBACK;
