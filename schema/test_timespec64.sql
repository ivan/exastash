BEGIN;

SELECT plan(1);

SELECT ok('epoch'::timestamptz::timespec64 = (0, 0)::timespec64);

SELECT * FROM finish();

ROLLBACK;
