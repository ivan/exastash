BEGIN;

SELECT plan(5);

CALL create_root_inode('fake_hostname', 41);

PREPARE cannot_insert_symlink_with_target_over_1024_bytes AS INSERT INTO symlinks (
    mtime, symlink_target, birth_time, birth_hostname, birth_version
) VALUES ((0, 0), repeat('x', 1025), (0, 0), 'fake', 41);
SELECT throws_like('cannot_insert_symlink_with_target_over_1024_bytes', '%violates check constraint%');

PREPARE insert_file AS INSERT INTO files (
    size, mtime, executable, birth_time, birth_hostname, birth_version
) VALUES (20, (0, 0), true, (0, 0), 'fake', 41);
SELECT lives_ok('insert_file');

PREPARE insert_dir AS INSERT INTO dirs (
    ino, mtime, birth_time, birth_hostname, birth_version
) VALUES (100, (0, 0), (0, 0), 'fake', 41);
SELECT lives_ok('insert_dir');

-- Parent the dir
CALL create_dirent(2, 'dir', 100);

PREPARE insert_symlink AS INSERT INTO symlinks (
    mtime, symlink_target, birth_time, birth_hostname, birth_version
) VALUES ((0, 0), '../some/target', (0, 0), 'fake', 41);
SELECT lives_ok('insert_symlink');

PREPARE insert_symlink_with_target_1024_bytes AS INSERT INTO symlinks (
    mtime, symlink_target, birth_time, birth_hostname, birth_version
) VALUES ((0, 0), repeat('x', 1024), (0, 0), 'fake', 41);
SELECT lives_ok('insert_symlink_with_target_1024_bytes');

--

SELECT * FROM finish();

ROLLBACK;
