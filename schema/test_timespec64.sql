BEGIN;

SELECT plan(3);

SELECT ok('epoch'::timestamptz::timespec64 = (0, 0)::timespec64);

SELECT ok(('epoch'::timestamptz + '1 hour'::interval)::timespec64 = (3600, 0)::timespec64);

SELECT ok(('epoch'::timestamptz + '1 hour 1 microsecond'::interval)::timespec64 = (3600, 1000)::timespec64);

SELECT * FROM finish();

ROLLBACK;
