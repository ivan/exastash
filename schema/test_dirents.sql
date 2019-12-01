BEGIN;

SELECT plan(3);

CALL create_root_inode('fake', 41);

-- CHECK constraints

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

SELECT * FROM finish();

ROLLBACK;
