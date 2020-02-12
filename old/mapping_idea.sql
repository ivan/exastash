-- We want a table per inode type, but we must never let ino's overlap
-- between tables.  Instead of using a trigger to ensure the other tables
-- do not have the ino, just use nonoverlapping ino ranges for each type.
--
-- inode map (inclusive ranges):
-- -512 * 2^54  to               1  [forbidden]
--           2  to        2^54 - 1  dirs
--        2^54  to  170 * 2^54 - 1  [reserved]
--  170 * 2^54  to  171 * 2^54 - 1  files
--  171 * 2^54  to  341 * 2^54 - 1  [reserved]
--  341 * 2^54  to  342 * 2^54 - 1  symlinks
--  342 * 2^54  to  512 * 2^54 - 1  [reserved]



-- inode 0 is not used by Linux filesystems (0 means NULL).
-- inode 1 is used by Linux filesystems for bad blocks information.
-- inode 2 is used for /
-- Start with inode 3 for all other inodes.
