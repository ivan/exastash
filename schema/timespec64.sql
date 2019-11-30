CREATE DOMAIN sec  AS bigint CHECK (VALUE >= 0);
CREATE DOMAIN nsec AS bigint CHECK (VALUE >= 0 AND VALUE <= 10 ^ 9);

-- We store timespec64 instead of timestamptz because timestamptz is only
-- microsecond precise, and some applications may reasonably expect nanosecond-
-- precise mtimes to round trip correctly.  It may also be useful in some cases
-- when sorting files created at nearly the same time.
CREATE TYPE timespec64 AS (
    sec  sec,
    nsec nsec
);

CREATE OR REPLACE FUNCTION timestamp_to_timespec64(timestamptz) RETURNS timespec64 AS $$
DECLARE
    epoch numeric;
BEGIN
    -- epoch: "For timestamp with time zone values, the number of seconds since
    -- 1970-01-01 00:00:00 UTC (can be negative)"
    --
    -- Convert to numeric for % 1 below
    epoch := extract(epoch from $1)::numeric;
    RETURN(SELECT (
        -- integer part
        floor(epoch),
        -- decimal part, times the number of nanoseconds in a second
        (epoch % 1) * 10 ^ 9
    )::timespec64);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE CAST (timestamptz AS timespec64) WITH FUNCTION timestamp_to_timespec64 AS ASSIGNMENT;
