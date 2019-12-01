BEGIN;

SELECT plan(1);

-- CHECK constraints

--INSERT INTO inodes (ino, type, size, mtime, executable) VALUES (20, 'REG', 0, (0, 0), false);

PREPARE child_cannot_be_parent AS INSERT INTO dirents (
  child, basename, parent
) VALUES (2, 'name', 2);
SELECT throws_ilike('child_cannot_be_parent', '%violates check constraint%');

-- TODO: ensure that parent is a DIR

SELECT * FROM finish();

ROLLBACK;
