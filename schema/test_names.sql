BEGIN;

SELECT plan(1);

-- CHECK constraints

PREPARE child_cannot_be_parent AS INSERT INTO names (
  child, name, parent
) VALUES (2, 'name', 2);
SELECT throws_ilike('child_cannot_be_parent', '%violates check constraint%');

-- TODO: ensure that parent is a DIR

SELECT * FROM finish();

ROLLBACK;
