BEGIN;

SELECT plan(18);

CALL create_root_inode('fake', 41);

PREPARE child_cannot_be_parent AS INSERT INTO dirents (
    parent, basename, child
) VALUES (2, 'name', 2);
SELECT throws_like('child_cannot_be_parent', '%violates check constraint%');

INSERT INTO inodes (
    ino, parent_ino, type, size, mtime, executable, symlink_target, birth_time, birth_hostname, birth_exastash_version
) VALUES
    (3, NULL, 'REG', 0,    (0, 0), false, NULL,        (0, 0), 'fake', 41),
    (4, NULL, 'REG', 0,    (0, 0), false, NULL,        (0, 0), 'fake', 41),
    (5, NULL, 'LNK', NULL, (0, 0), NULL,  'somewhere', (0, 0), 'fake', 41),
    (6, NULL, 'DIR', NULL, (0, 0), NULL,  NULL,        (0, 0), 'fake', 41),
    (7, NULL, 'DIR', NULL, (0, 0), NULL,  NULL,        (0, 0), 'fake', 41);
INSERT INTO dirents (parent, basename, child) VALUES (2, '7', 7);

PREPARE parent_must_exist AS INSERT INTO dirents (
    parent, basename, child
) VALUES (300, 'name', 4);
SELECT throws_like('parent_must_exist', 'parent ino=300 does not exist in inodes');

PREPARE child_must_exist AS INSERT INTO dirents (
    parent, basename, child
) VALUES (2, 'name', 300);
SELECT throws_like('child_must_exist', 'child ino=300 does not exist in inodes');

PREPARE parent_cannot_be_a_reg AS INSERT INTO dirents (
    parent, basename, child
) VALUES (3, 'name', 4);
SELECT throws_like('parent_cannot_be_a_reg', 'parent ino=3 is not a DIR');

PREPARE parent_cannot_be_a_lnk AS INSERT INTO dirents (
    parent, basename, child
) VALUES (5, 'name', 4);
SELECT throws_like('parent_cannot_be_a_lnk', 'parent ino=5 is not a DIR');

PREPARE can_add_reg_child AS INSERT INTO dirents (
    parent, basename, child
) VALUES (2, 'child', 3);
SELECT lives_ok('can_add_reg_child');

PREPARE can_delete_reg_child AS DELETE FROM dirents WHERE parent = 2 AND basename = 'child';
SELECT lives_ok('can_delete_reg_child');

PREPARE can_add_reg_child_weird_name AS INSERT INTO dirents (
    parent, basename, child
) VALUES (2, '.child..with.dots.', 3);
SELECT lives_ok('can_add_reg_child_weird_name');

PREPARE can_add_lnk_child AS INSERT INTO dirents (
    parent, basename, child
) VALUES (2, 'symlink', 5);
SELECT lives_ok('can_add_lnk_child');

INSERT INTO dirents (parent, basename, child) VALUES (2, 'same_parent_basename', 4);
INSERT INTO dirents (parent, basename, child) VALUES (7, 'same_parent_basename', 4); -- different parent is OK
PREPARE cannot_add_same_parent_basename AS INSERT INTO dirents (
    parent, basename, child
) VALUES (2, 'same_parent_basename', 4);
SELECT throws_like('cannot_add_same_parent_basename', 'duplicate key value violates unique constraint%');

PREPARE cannot_add_empty_basename AS INSERT INTO dirents (
    parent, basename, child
) VALUES (2, '', 4);
SELECT throws_like('cannot_add_empty_basename', '%violates check constraint%');

PREPARE cannot_add_dot_basename AS INSERT INTO dirents (
    parent, basename, child
) VALUES (2, '.', 4);
SELECT throws_like('cannot_add_dot_basename', '%violates check constraint%');

PREPARE cannot_add_dot_dot_basename AS INSERT INTO dirents (
    parent, basename, child
) VALUES (2, '..', 4);
SELECT throws_like('cannot_add_dot_dot_basename', '%violates check constraint%');

-- Directories

PREPARE cannot_create_children_for_unparented_dir AS INSERT INTO dirents (
    parent, basename, child
) VALUES (6, 'name', 3);
SELECT throws_like('cannot_create_children_for_unparented_dir', 'cannot create dirents for DIR ino=6 with no parent');

INSERT INTO dirents (parent, basename, child) VALUES (2, 'dir', 6);

PREPARE cannot_update AS UPDATE dirents SET basename = 'renamed' WHERE parent = 2 AND child = 6;
SELECT throws_like('cannot_update', 'cannot change parent, basename, or child');

PREPARE can_add_file_to_nonroot_dir AS INSERT INTO dirents (
    parent, basename, child
) VALUES (6, 'name', 3);
SELECT lives_ok('can_add_file_to_nonroot_dir');

PREPARE cannot_delete_nonempty_dir AS DELETE FROM dirents WHERE parent = 2 AND basename = 'dir';
SELECT throws_like('cannot_delete_nonempty_dir', 'child DIR ino=6 is not empty');

PREPARE can_remove_dirent_from_nonroot_dir AS DELETE FROM dirents WHERE parent = 6 AND basename = 'name';
SELECT lives_ok('can_remove_dirent_from_nonroot_dir');

SELECT * FROM finish();

ROLLBACK;
