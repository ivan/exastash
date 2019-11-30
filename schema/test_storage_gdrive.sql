BEGIN;

SELECT plan(9);

INSERT INTO gdrive_domains (gdrive_domain) VALUES ('example.org');
INSERT INTO gdrive_files (file_id, file_owner, md5, crc32c, size)
    VALUES ('AAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'some@user', '\xd41d8cd98f00b204e9800998ecf8427e', '\x00000000', 1);
INSERT INTO gdrive_files (file_id, file_owner, md5, crc32c, size)
    VALUES ('AAAAAAAAAAAAAAAAAAAAAAAAAAAA2', 'some@user', '\xd41d8cd98f00b204e9800998ecf8427e', '\x00000000', 1);

-- Errors

PREPARE cannot_create_empty_chunk_sequence AS INSERT INTO gdrive_chunk_sequences (files, aes_key)
    VALUES (ARRAY[]::file_id[], '\x00000000000000000000000000000000');
SELECT throws_ilike('cannot_create_empty_chunk_sequence', '%violates check constraint%');

PREPARE cannot_reference_nonexistent_files AS INSERT INTO gdrive_chunk_sequences (files, aes_key)
    VALUES (ARRAY['BAAAAAAAAAAAAAAAAAAAAAAAAAAAA']::file_id[], '\x00000000000000000000000000000000');
SELECT throws_ilike('cannot_reference_nonexistent_files', '%only 0 of these are in gdrive_files%');

PREPARE cannot_reference_nonexistent_files2 AS INSERT INTO gdrive_chunk_sequences (files, aes_key)
    VALUES (ARRAY['AAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'BAAAAAAAAAAAAAAAAAAAAAAAAAAAA']::file_id[], '\x00000000000000000000000000000000');
SELECT throws_ilike('cannot_reference_nonexistent_files2', '%only 1 of these are in gdrive_files%');

PREPARE cannot_reference_same_file_more_than_once AS INSERT INTO gdrive_chunk_sequences (files, aes_key)
    VALUES (ARRAY['AAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAA']::file_id[], '\x00000000000000000000000000000000');
SELECT throws_ilike('cannot_reference_same_file_more_than_once', '%only 1 of these are in gdrive_files%');

PREPARE cannot_insert_without_aes_key AS INSERT INTO gdrive_chunk_sequences (files)
    VALUES (ARRAY['AAAAAAAAAAAAAAAAAAAAAAAAAAAAA']::file_id[]);
SELECT throws_ilike('cannot_insert_without_aes_key', '%violates not-null constraint%');

PREPARE cannot_insert_wrong_aes_key_size_15 AS INSERT INTO gdrive_chunk_sequences (files, aes_key)
    VALUES (ARRAY['AAAAAAAAAAAAAAAAAAAAAAAAAAAAA']::file_id[], '\x000000000000000000000000000000');
SELECT throws_ilike('cannot_insert_wrong_aes_key_size_15', '%violates check constraint%');

PREPARE cannot_insert_wrong_aes_key_size_17 AS INSERT INTO gdrive_chunk_sequences (files, aes_key)
    VALUES (ARRAY['AAAAAAAAAAAAAAAAAAAAAAAAAAAAA']::file_id[], '\x0000000000000000000000000000000000');
SELECT throws_ilike('cannot_insert_wrong_aes_key_size_17', '%violates check constraint%');

-- Successes

PREPARE one_file_sequence AS INSERT INTO gdrive_chunk_sequences (files, aes_key)
    VALUES (ARRAY['AAAAAAAAAAAAAAAAAAAAAAAAAAAAA']::file_id[], '\x00000000000000000000000000000000');
SELECT lives_ok('one_file_sequence');

PREPARE two_file_sequence AS INSERT INTO gdrive_chunk_sequences (files, aes_key)
    VALUES (ARRAY['AAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAA2']::file_id[], '\x00000000000000000000000000000000');
SELECT lives_ok('two_file_sequence');

--

SELECT * FROM finish();

ROLLBACK;
