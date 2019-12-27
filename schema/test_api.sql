BEGIN;

SELECT plan(6);

CALL create_root_inode('fake_hostname', 41);

INSERT INTO inodes (
    ino, parent_ino, type, size, mtime, executable, symlink_target, birth_time, birth_hostname, birth_exastash_version
) VALUES
    (3, NULL, 'REG', 0,    (0, 0), false, NULL,        (0, 0), 'fake_hostname', 41),
    (4, NULL, 'REG', 0,    (0, 0), false, NULL,        (0, 0), 'fake_hostname', 41),
    (5, NULL, 'LNK', NULL, (0, 0), NULL,  'somewhere', (0, 0), 'fake_hostname', 41),
    (6, NULL, 'DIR', NULL, (0, 0), NULL,  NULL,        (0, 0), 'fake_hostname', 41),
    (7, NULL, 'DIR', NULL, (0, 0), NULL,  NULL,        (0, 0), 'fake_hostname', 41);

INSERT INTO dirents (parent, basename, child) VALUES (2, 'six', 6);
INSERT INTO dirents (parent, basename, child) VALUES (6, 'seven', 7);
INSERT INTO dirents (parent, basename, child) VALUES (7, 'three', 3);

PREPARE get_nonexistent AS SELECT get_ino_for_path(2, '/nonexistent');
SELECT throws_like('get_nonexistent', 'inode 2 does not have dirent for ''nonexistent''');

SELECT ok((SELECT get_ino_for_path(2, '/')) = 2);
SELECT ok((SELECT get_ino_for_path(2, '/.')) = 2);
SELECT ok((SELECT get_ino_for_path(2, '/./')) = 2);
SELECT ok((SELECT get_ino_for_path(2, '/..')) = 2);
SELECT ok((SELECT get_ino_for_path(2, '/../')) = 2);

SELECT * FROM finish();

ROLLBACK;
