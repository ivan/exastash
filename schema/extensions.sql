CREATE EXTENSION IF NOT EXISTS periods VERSION '1.2' CASCADE;
-- GRANT USAGE ON SCHEMA periods TO archive;

CREATE EXTENSION IF NOT EXISTS pg_ivm VERSION '1.2' CASCADE;
-- https://github.com/sraoss/pg_ivm/issues/25
-- GRANT ALL ON TABLE pg_catalog.pg_ivm_immv TO archive;

CREATE EXTENSION IF NOT EXISTS pgroonga;
