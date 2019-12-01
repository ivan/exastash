BEGIN;

SELECT plan(6);

CALL create_root_inode('fake', 41);

PREPARE child_cannot_be_parent AS INSERT INTO dirents (
    parent, basename, child
) VALUES (2, 'name', 2);
SELECT throws_ilike('child_cannot_be_parent', '%violates check constraint%');

INSERT INTO inodes (
    ino, type, size, mtime, executable, symlink_target, birth_time, birth_hostname, birth_exastash_version
) VALUES (3, 'REG', 0, (0, 0), false, NULL, (0, 0), 'fake', 41);
INSERT INTO inodes (
    ino, type, size, mtime, executable, symlink_target, birth_time, birth_hostname, birth_exastash_version
) VALUES (4, 'REG', 0, (0, 0), false, NULL, (0, 0), 'fake', 41);
INSERT INTO inodes (
    ino, type, size, mtime, executable, symlink_target, birth_time, birth_hostname, birth_exastash_version
) VALUES (5, 'LNK', NULL, (0, 0), NULL, 'somewhere', (0, 0), 'fake', 41);

PREPARE parent_cannot_be_a_reg AS INSERT INTO dirents (
    parent, basename, child
) VALUES (3, 'name', 4);
SELECT throws_ilike('parent_cannot_be_a_reg', 'parent ino=3 is not a DIR');

PREPARE parent_cannot_be_a_lnk AS INSERT INTO dirents (
    parent, basename, child
) VALUES (5, 'name', 4);
SELECT throws_ilike('parent_cannot_be_a_lnk', 'parent ino=5 is not a DIR');

PREPARE can_add_reg_child AS INSERT INTO dirents (
    parent, basename, child
) VALUES (2, 'child', 3);
SELECT lives_ok('can_add_reg_child');

PREPARE can_add_lnk_child AS INSERT INTO dirents (
    parent, basename, child
) VALUES (2, 'symlink', 5);
SELECT lives_ok('can_add_lnk_child');

PREPARE cannot_add_same_basename AS INSERT INTO dirents (
    parent, basename, child
) VALUES (2, 'child', 4);
SELECT throws_ilike('cannot_add_same_basename', 'duplicate key value violates unique constraint%');

SELECT * FROM finish();

ROLLBACK;
