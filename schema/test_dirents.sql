BEGIN;

SELECT plan(1);

CALL create_root_inode('fake', 41);

-- CHECK constraints

PREPARE child_cannot_be_parent AS INSERT INTO dirents (
    child, basename, parent
) VALUES (2, 'name', 2);
SELECT throws_ilike('child_cannot_be_parent', '%violates check constraint%');

-- TODO: ensure that parent is a DIR

SELECT * FROM finish();

ROLLBACK;
