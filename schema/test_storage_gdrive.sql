BEGIN;

SELECT plan(1);

INSERT INTO gdrive_domains (gdrive_domain) VALUES ('example.org');
INSERT INTO gdrive_files (file_id, file_owner, md5, crc32c, size)
    VALUES ('AAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'some@user', '\xd41d8cd98f00b204e9800998ecf8427e', '\x00000000', 1);

-- Errors

-- ino 2 is the root directory
PREPARE cannot_create_empty_chunk_sequence AS INSERT INTO gdrive_chunk_sequences (files) VALUES (ARRAY[]::file_id[]);
SELECT throws_ilike('cannot_create_empty_chunk_sequence', '%violates check constraint%');

-- Successes



--

SELECT * FROM finish();

ROLLBACK;
