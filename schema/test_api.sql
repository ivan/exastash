BEGIN;

SELECT plan(28);

CALL create_root_inode('fake_hostname', 41);

INSERT INTO inodes (
    ino, parent_ino, type, size, mtime, executable, symlink_target, birth_time, birth_hostname, birth_exastash_version
) VALUES
    (3,  NULL, 'REG', 0,    (0, 0), false, NULL,        (0, 0), 'fake_hostname', 41),
    (4,  NULL, 'REG', 0,    (0, 0), false, NULL,        (0, 0), 'fake_hostname', 41),
    (6,  NULL, 'DIR', NULL, (0, 0), NULL,  NULL,        (0, 0), 'fake_hostname', 41),
    (7,  NULL, 'DIR', NULL, (0, 0), NULL,  NULL,        (0, 0), 'fake_hostname', 41),
    (8,  NULL, 'LNK', NULL, (0, 0), NULL,  'six',       (0, 0), 'fake_hostname', 41),
    (9,  NULL, 'LNK', NULL, (0, 0), NULL,  '../six',    (0, 0), 'fake_hostname', 41),
    (10, NULL, 'LNK', NULL, (0, 0), NULL,  '../../six', (0, 0), 'fake_hostname', 41),
    (11, NULL, 'LNK', NULL, (0, 0), NULL,  '../',       (0, 0), 'fake_hostname', 41),
    (12, NULL, 'LNK', NULL, (0, 0), NULL,  'a',         (0, 0), 'fake_hostname', 41),
    (13, NULL, 'LNK', NULL, (0, 0), NULL,  'b',         (0, 0), 'fake_hostname', 41);

INSERT INTO dirents (parent, basename, child) VALUES
    (2, 'six', 6),
    (2, 'symlink8',  8),
    (2, 'symlink9',  9),
    (2, 'symlink10', 10),
    (2, 'three', 3),
    (6, 'seven', 7),
    -- same symlink inode but parented in different place, so pointing to different absolute paths
    (7, 'symlink11', 11),
    (6, 'symlink11', 11),
    (7, 'three', 3),
    (2, 'b', 12), -- symlink b -> a
    (2, 'a', 13); -- symlink a -> b

PREPARE get_nonexistent AS SELECT get_ino_for_path(2, '/nonexistent');
SELECT throws_like('get_nonexistent', 'inode 2 does not have dirent for ''nonexistent''');

SELECT ok((SELECT get_ino_for_path(2, '/')) = 2);
SELECT ok((SELECT get_ino_for_path(2, '/.')) = 2);
SELECT ok((SELECT get_ino_for_path(2, '/./')) = 2);
SELECT ok((SELECT get_ino_for_path(2, '/..')) = 2);
SELECT ok((SELECT get_ino_for_path(2, '/../')) = 2);

SELECT ok((SELECT get_ino_for_path(2, '/six')) = 6);
SELECT ok((SELECT get_ino_for_path(2, '/six/')) = 6);
SELECT ok((SELECT get_ino_for_path(2, '/three')) = 3);

PREPARE cannot_add_trailing_slash_to_reg AS SELECT get_ino_for_path(2, '/three/');
SELECT throws_like('cannot_add_trailing_slash_to_reg', 'inode 3 is not a directory');

SELECT ok((SELECT get_ino_for_path(2, '/six/seven')) = 7);
SELECT ok((SELECT get_ino_for_path(2, '/six/seven/')) = 7);
SELECT ok((SELECT get_ino_for_path(2, '/six/seven/.')) = 7);
SELECT ok((SELECT get_ino_for_path(2, '/six/seven/..')) = 6);
SELECT ok((SELECT get_ino_for_path(2, '/six/seven/../')) = 6);
SELECT ok((SELECT get_ino_for_path(2, '/six/seven/../..')) = 2);
SELECT ok((SELECT get_ino_for_path(2, '/six/seven/../../')) = 2);
SELECT ok((SELECT get_ino_for_path(2, '/six/seven/../../..')) = 2);

SELECT ok((SELECT get_ino_for_path(2, 'symlink8'))  = 6);
SELECT ok((SELECT get_ino_for_path(2, 'symlink9'))  = 6);
SELECT ok((SELECT get_ino_for_path(2, 'symlink10')) = 6);
SELECT ok((SELECT get_ino_for_path(2, 'six/seven/symlink11')) = 6);
SELECT ok((SELECT get_ino_for_path(2, 'six/seven/symlink11/symlink11')) = 2);
SELECT ok((SELECT get_ino_for_path(2, 'six/seven/symlink11/symlink11/six')) = 6);
SELECT ok((SELECT get_ino_for_path(2, 'six/seven/symlink11/symlink11/six/seven')) = 7);
SELECT ok((SELECT get_ino_for_path(2, 'six/seven/symlink11/symlink11/six/seven/symlink11')) = 6);
SELECT ok((SELECT get_ino_for_path(2, 'six/seven/symlink11/symlink11/six/seven/symlink11/symlink11')) = 2);

PREPARE too_many_levels_of_symbolic_links AS SELECT get_ino_for_path(2, '/a');
SELECT throws_like('too_many_levels_of_symbolic_links', 'Too many levels of symbolic links');

SELECT * FROM finish();

ROLLBACK;
