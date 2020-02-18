BEGIN;

SELECT plan(14);

INSERT INTO gsuite_domains (gsuite_domain) VALUES ('example.org');
INSERT INTO gdrive_files (file_id, file_owner, md5, crc32c, size)
    VALUES ('AAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'some@user', '\xd41d8cd98f00b204e9800998ecf8427e', '\x00000000', 1);
INSERT INTO gdrive_files (file_id, file_owner, md5, crc32c, size)
    VALUES ('AAAAAAAAAAAAAAAAAAAAAAAAAAAA2', 'some@user', '\xd41d8cd98f00b204e9800998ecf8427e', '\x00000000', 1);

PREPARE cannot_create_empty_chunk_sequence AS INSERT INTO gdrive_chunk_sequences (files, cipher, cipher_key)
    VALUES (ARRAY[]::file_id[], 'AES_128_GCM', '\x00000000000000000000000000000000');
SELECT throws_like('cannot_create_empty_chunk_sequence', '%violates check constraint%');

PREPARE cannot_reference_nonexistent_files AS INSERT INTO gdrive_chunk_sequences (files, cipher, cipher_key)
    VALUES (ARRAY['BAAAAAAAAAAAAAAAAAAAAAAAAAAAA']::file_id[], 'AES_128_GCM', '\x00000000000000000000000000000000');
SELECT throws_like('cannot_reference_nonexistent_files', '%only 0 of these are in gdrive_files%');

PREPARE cannot_reference_nonexistent_files2 AS INSERT INTO gdrive_chunk_sequences (files, cipher, cipher_key)
    VALUES (ARRAY['AAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'BAAAAAAAAAAAAAAAAAAAAAAAAAAAA']::file_id[], 'AES_128_GCM', '\x00000000000000000000000000000000');
SELECT throws_like('cannot_reference_nonexistent_files2', '%only 1 of these are in gdrive_files%');

PREPARE cannot_reference_same_file_more_than_once AS INSERT INTO gdrive_chunk_sequences (files, cipher, cipher_key)
    VALUES (ARRAY['AAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAA']::file_id[], 'AES_128_GCM', '\x00000000000000000000000000000000');
SELECT throws_like('cannot_reference_same_file_more_than_once', '%only 1 of these are in gdrive_files%');

PREPARE cannot_insert_without_cipher_key AS INSERT INTO gdrive_chunk_sequences (files)
    VALUES (ARRAY['AAAAAAAAAAAAAAAAAAAAAAAAAAAAA']::file_id[]);
SELECT throws_like('cannot_insert_without_cipher_key', '%violates not-null constraint%');

PREPARE cannot_insert_wrong_cipher_key_size_15 AS INSERT INTO gdrive_chunk_sequences (files, cipher, cipher_key)
    VALUES (ARRAY['AAAAAAAAAAAAAAAAAAAAAAAAAAAAA']::file_id[], 'AES_128_GCM', '\x000000000000000000000000000000');
SELECT throws_like('cannot_insert_wrong_cipher_key_size_15', '%violates check constraint%');

PREPARE cannot_insert_wrong_cipher_key_size_17 AS INSERT INTO gdrive_chunk_sequences (files, cipher, cipher_key)
    VALUES (ARRAY['AAAAAAAAAAAAAAAAAAAAAAAAAAAAA']::file_id[], 'AES_128_GCM', '\x0000000000000000000000000000000000');
SELECT throws_like('cannot_insert_wrong_cipher_key_size_17', '%violates check constraint%');

PREPARE one_file_sequence AS INSERT INTO gdrive_chunk_sequences (files, cipher, cipher_key)
    VALUES (ARRAY['AAAAAAAAAAAAAAAAAAAAAAAAAAAAA']::file_id[], 'AES_128_GCM', '\x00000000000000000000000000000000');
SELECT lives_ok('one_file_sequence');

PREPARE two_file_sequence AS INSERT INTO gdrive_chunk_sequences (files, cipher, cipher_key)
    VALUES (ARRAY['AAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAA2']::file_id[], 'AES_128_CTR', '\x00000000000000000000000000000001');
SELECT lives_ok('two_file_sequence');

-- Files can't be deleted if they're still referenced by a sequence

PREPARE cannot_delete_file_if_still_referenced AS DELETE FROM gdrive_files WHERE file_id = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAA';
SELECT throws_like('cannot_delete_file_if_still_referenced', '%file_id still referenced%');

PREPARE cannot_delete_file_if_still_referenced2 AS DELETE FROM gdrive_files WHERE file_id = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAA2';
SELECT throws_like('cannot_delete_file_if_still_referenced2', '%file_id still referenced%');

-- Sequence can be deleted

PREPARE delete_sequence AS DELETE FROM gdrive_chunk_sequences;
SELECT lives_ok('delete_sequence');

-- Files can be deleted now that they're no longer referenced

PREPARE can_delete_file1 AS DELETE FROM gdrive_files WHERE file_id = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAA';
SELECT lives_ok('can_delete_file1');

PREPARE can_delete_file2 AS DELETE FROM gdrive_files WHERE file_id = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAA2';
SELECT lives_ok('can_delete_file2');

--

SELECT * FROM finish();

ROLLBACK;
