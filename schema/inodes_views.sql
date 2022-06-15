-- File ids for files that are _not_ missing storage
CREATE VIEW file_ids_with_storage_or_zero_size AS
    SELECT file_id AS id FROM storage_fofs
    UNION
    SELECT file_id AS id FROM storage_inline
    UNION
    SELECT file_id AS id FROM storage_gdrive
    UNION
    SELECT file_id AS id FROM storage_internetarchive
    UNION
    SELECT id FROM files WHERE size = 0;

-- Like file_ids_with_storage_or_zero_size, but with possible duplicates.
-- UNION ALL is faster than UNION.
CREATE VIEW file_ids_with_storage_or_zero_size_with_duplicates AS
    SELECT file_id AS id FROM storage_fofs
    UNION ALL
    SELECT file_id AS id FROM storage_inline
    UNION ALL
    SELECT file_id AS id FROM storage_gdrive
    UNION ALL
    SELECT file_id AS id FROM storage_internetarchive
    UNION ALL
    SELECT id FROM files WHERE size = 0;

-- Like the files table, but containing only files that are missing storage.
CREATE VIEW files_missing_storage AS
    SELECT * FROM files WHERE id IN (
        SELECT id FROM files
        EXCEPT ALL -- faster than EXCEPT
        SELECT id FROM file_ids_with_storage_or_zero_size_with_duplicates
    );

-- Like the files table, but containing only files that are missing storage but _do_ have a dirent.
CREATE VIEW files_with_dirents_missing_storage AS
    SELECT * FROM files WHERE id IN (
        SELECT child_file AS id FROM dirents
        EXCEPT ALL -- faster than EXCEPT
        SELECT id FROM file_ids_with_storage_or_zero_size_with_duplicates
    );
