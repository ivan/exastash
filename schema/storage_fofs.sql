-- Piles are the root directories containing cells. There should normally be one
-- pile per filesystem/storage device.
CREATE TABLE piles (
    -- Limit of 1M can be raised if needed
    id                    int       GENERATED ALWAYS AS IDENTITY PRIMARY KEY CHECK (id >= 1 AND id < 1000000),
    -- The number of files to place in each cell before marking it full and making a new cell.
    -- For performance reasons, this is not strictly enforced unless fullness_check_ratio = 1;
    -- the cell may go over the threshold.
    --
    -- A typical value is 10000.
    files_per_cell        int       NOT NULL CHECK (files_per_cell >= 1),
    -- The machine on which the pile is stored
    hostname              hostname  NOT NULL,
    -- The absolute path to the root directory of the pile on the machine, not including the automatically suffixed /{id}
    "path"                text      NOT NULL CHECK ("path" ~ '\A/.*[^/]\Z'), -- Must start with /, must not end with /
    -- How often to check whether the cell has reached capacity before marking it full; 0 = never, 1 = always
    --
    -- For files_per_cell = 10000, a typical value for fullness_check_ratio is 0.01,
    -- thus causing ~100 listdir calls on a 10000-sized cell as it grows to capacity.
    fullness_check_ratio  numeric   NOT NULL CHECK (fullness_check_ratio >= 0 AND fullness_check_ratio <= 1)
);

CREATE TRIGGER piles_check_update
    BEFORE UPDATE ON piles
    FOR EACH ROW
    WHEN (OLD.id != NEW.id)
    EXECUTE FUNCTION raise_exception('cannot change id');

CREATE TRIGGER piles_forbid_truncate
    BEFORE TRUNCATE ON piles
    EXECUTE FUNCTION raise_exception('truncate is forbidden');



-- Cells are the directories containing stash files.
CREATE TABLE cells (
    -- Limit of 200M can be raised if needed
    id            int       GENERATED ALWAYS AS IDENTITY PRIMARY KEY CHECK (id >= 1 AND id < 200000000),
    -- The pile we are parented in
    pile_id       int       NOT NULL REFERENCES piles (id),
    -- Whether we are full because we have reached the per-cell file limit
    "full"        boolean   NOT NULL DEFAULT FALSE
);

-- Make it fast to find the not-full cells.
-- Most cells will be full, because we do not create a new cell until the existing cells in the pile are full.
CREATE INDEX ON cells ("full") WHERE "full" = FALSE;

CREATE TRIGGER cells_check_update
    BEFORE UPDATE ON cells
    FOR EACH ROW
    WHEN (OLD.id != NEW.id)
    EXECUTE FUNCTION raise_exception('cannot change id');

CREATE TRIGGER cells_forbid_truncate
    BEFORE TRUNCATE ON cells
    EXECUTE FUNCTION raise_exception('truncate is forbidden');

-- Set the index to use for future CLUSTER operations
ALTER TABLE cells CLUSTER ON cells_pkey;



CREATE TABLE storage_fofs (
    file_id       bigint  NOT NULL REFERENCES files (id),
    -- The cell in which the file is stored
    cell_id       int     NOT NULL REFERENCES cells (id),

    -- A file can be stored in multiple cells (because we may want to store it on multiple machines)
    PRIMARY KEY (file_id, cell_id)
);

CREATE TRIGGER storage_fofs_check_update
    BEFORE UPDATE ON storage_fofs
    FOR EACH ROW
    EXECUTE FUNCTION raise_exception('cannot change file_id or cell_id');

CREATE TRIGGER storage_fofs_forbid_truncate
    BEFORE TRUNCATE ON storage_fofs
    EXECUTE FUNCTION raise_exception('truncate is forbidden');

-- Set the index to use for future CLUSTER operations
ALTER TABLE storage_fofs CLUSTER ON storage_fofs_pkey;
