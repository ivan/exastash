BEGIN;

SELECT plan(6);

INSERT INTO gdrive_domains (gdrive_domain) VALUES ('example.org');
INSERT INTO gdrive_files (file_id, file_owner, md5, crc32c, size)
    VALUES ('AAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'some@user', '\xd41d8cd98f00b204e9800998ecf8427e', '\x00000000', 1);
INSERT INTO gdrive_files (file_id, file_owner, md5, crc32c, size)
    VALUES ('AAAAAAAAAAAAAAAAAAAAAAAAAAAA2', 'some@user', '\xd41d8cd98f00b204e9800998ecf8427e', '\x00000000', 1);

-- Errors

PREPARE cannot_create_empty_chunk_sequence AS INSERT INTO gdrive_chunk_sequences (files) VALUES (ARRAY[]::file_id[]);
SELECT throws_ilike('cannot_create_empty_chunk_sequence', '%violates check constraint%');

PREPARE cannot_reference_nonexistent_files AS INSERT INTO gdrive_chunk_sequences (files) VALUES (ARRAY['BAAAAAAAAAAAAAAAAAAAAAAAAAAAA']::file_id[]);
SELECT throws_ilike('cannot_reference_nonexistent_files', '%only 0 of these are in gdrive_files%');

PREPARE cannot_reference_nonexistent_files2 AS INSERT INTO gdrive_chunk_sequences (files) VALUES (ARRAY['AAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'BAAAAAAAAAAAAAAAAAAAAAAAAAAAA']::file_id[]);
SELECT throws_ilike('cannot_reference_nonexistent_files2', '%only 1 of these are in gdrive_files%');

PREPARE cannot_reference_same_file_more_than_once AS INSERT INTO gdrive_chunk_sequences (files) VALUES (ARRAY['AAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAA']::file_id[]);
SELECT throws_ilike('cannot_reference_same_file_more_than_once', '%only 1 of these are in gdrive_files%');

-- Successes

PREPARE one_file_sequence AS INSERT INTO gdrive_chunk_sequences (files) VALUES (ARRAY['AAAAAAAAAAAAAAAAAAAAAAAAAAAAA']::file_id[]);
SELECT lives_ok('one_file_sequence');

PREPARE two_file_sequence AS INSERT INTO gdrive_chunk_sequences (files) VALUES (ARRAY['AAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAA2']::file_id[]);
SELECT lives_ok('two_file_sequence');

--

SELECT * FROM finish();

ROLLBACK;
