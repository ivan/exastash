CREATE TABLE exastash_versions (
    id       smallint  PRIMARY KEY,
    version  text      UNIQUE NOT NULL CHECK (version ~ '\A.{3,64}\Z')
);

INSERT INTO exastash_versions (id, version) VALUES
    -- terastash
    (1,  '0.5.2D2015-09-19T04:35:32.070511779Z'),
    (2,  '0.5.2D2015-09-19T04:39:36.883634174Z'),
    (3,  '0.5.3D2015-09-19T22:17:23.172582820Z'),
    (4,  '0.5.4D2015-09-20T06:30:58.594089122Z'),
    (5,  '0.5.4D2015-09-20T07:18:03.104669434Z'),
    (6,  '0.5.4D2015-09-20T18:17:20.369537235Z'),
    (7,  '0.5.5D2015-09-25T20:08:30.678255291Z'),
    (8,  '0.5.5D2015-09-27T23:25:32.360875640Z'),
    (9,  '0.5.5D2015-09-29T05:12:21.971729503Z'),
    (10, '0.5.6D2015-10-01T08:47:10.159309646Z'),
    (11, '0.5.6D2015-10-02T18:47:58.855768569Z'),
    (12, '0.5.6D2015-10-03T06:52:22.531097900Z'),
    (13, '0.5.6D2015-10-03T20:08:30.410609268Z'),
    (14, '0.5.7D2015-10-03T23:50:28.027358942Z'),
    (15, '0.5.7D2015-10-04T00:16:00.301652597Z'),
    (16, '0.5.7D2015-10-04T21:53:44.793162176Z'),
    (17, '0.5.8D2015-10-12T00:31:37.469228626Z'),
    (18, '0.5.8D2015-10-19T00:30:48.421188465Z'),
    (19, '0.6.0D2015-10-19T00:30:48.421188465Z'),
    (20, '0.6.2D2015-10-19T00:30:48.421188465Z'),
    (21, '0.6.4D2015-10-19T00:30:48.421188465Z'),
    (22, '0.6.5D2015-10-19T00:30:48.421188465Z'),
    (23, '0.6.7D2015-10-19T00:30:48.421188465Z'),
    (24, '1.0.0D2018-03-26T08:30:43.135238086Z'),
    (25, '1.0.1D2018-03-26T09:41:06.425062593Z'),
    (26, '1.0.2D2018-03-26T10:56:27.031710801Z'),
    (27, '1.0.4D2018-03-29T10:15:46.975066434Z'),
    (28, '1.0.5D2018-03-29T10:50:05.736647037Z'),
    (29, '1.0.5D2018-03-29T10:51:17.155142289Z'),
    (30, '1.0.7D2018-03-29T10:51:17.155142289Z'),
    (31, '1.0.7D2018-03-29T15:43:38.009191939Z'),
    (32, '1.0.7D2018-07-18T09:44:35.358534591Z'),
    (33, '1.1.0D2018-12-29T05:51:00.217775026Z'),
    (34, '1.2.0D2019-02-26T06:20:33.409736396Z'),
    (35, '1.3.0D2019-06-01T17:48:52.642718754Z'),
    (36, '1.4.1D2019-06-09T00:46:07.805779595Z'),
    (37, '1.4.2D2019-06-10T23:36:57.953001081Z'),
    (38, '1.4.3D2019-06-11T03:24:57.091314301Z'),
    (39, '1.4.4D2019-06-13T01:39:37.950605456Z'),
    (40, '1.5.0D2019-11-21T13:09:56.828247932Z'),

    -- exastash
    (41, '2.0.0'),
    (42, '2.0.1'),
    (43, '2.0.3'),
    (44, '2.0.4'),
    (45, '2.0.5'),
    (46, '2.1.0'),
    (47, '2.2.0'),
    (48, '2.2.1'),
    (49, '2.2.2'),
    (50, '2.2.3'),
    (51, '2.2.4'),
    (52, '2.2.5'),
    (53, '2.2.6'),
    (54, '2.2.7'),
    (55, '2.2.8'),
    (56, '2.2.9'),
    (57, '2.2.10'),
    (58, '2.2.11'),
    (59, '2.2.12'),
    (60, '2.3.0'),
    (61, '2.3.1'),
    (62, '2.4.0'),
    (63, '2.4.1'),
    (64, '2.4.2'),
    (65, '2.4.3'),
    (66, '2.4.4'),
    (67, '2.4.5'),
    (68, '2.4.6'),
    (69, '2.4.7'),
    (70, '2.5.0'),
    (71, '2.5.1'),
    (72, '2.5.2'),
    (73, '2.5.3'),
    (74, '2.5.4'),
    (75, '3.0.0'),
    (76, '3.0.1'),
    (77, '3.0.2'),
    (78, '3.0.3'),
    (79, '3.0.4'),
    (80, '3.0.5'),
    (81, '3.0.6'),
    (82, '3.0.7'),
    (83, '3.0.8'),
    (84, '3.0.9'),
    (85, '3.1.0'),
    (86, '3.1.1'),
    (87, '3.1.2'),
    (88, '3.1.3'),
    (89, '3.1.4'),
    (90, '3.1.5'),
    (91, '3.1.6'),
    (92, '3.1.7'),
    (93, '3.1.8'),
    (94, '3.1.9'),
    (95, '3.2.0'),
    (96, '3.2.1'),
    (97, '3.2.2'),
    (98, '3.2.3'),
    (99, '3.3.0'),
    (100, '3.3.1'),
    (101, '3.4.0'),
    (102, '3.4.1'),
    (103, '3.4.2'),
    (104, '3.4.3'),
    (105, '3.4.4'),
    (106, '3.4.5'),
    (107, '3.5.0'),
    (108, '3.5.1'),
    (109, '3.5.2'),
    (110, '3.5.3'),
    (111, '3.5.4'),
    (112, '3.5.5'),
    (113, '3.5.6'),
    (114, '3.5.7'),
    (115, '3.5.8'),
    (116, '3.5.9'),
    (117, '3.6.0'),
    (118, '3.6.1'),
    (119, '3.7.0'),
    (120, '3.7.1'),
    (121, '3.7.2'),
    (122, '3.7.3'),
    (123, '3.7.4'),
    (124, '3.7.5'),
    (125, '3.7.6'),
    (126, '3.7.7'),
    (127, '3.7.8'),
    (128, '3.8.0'),
    (129, '3.8.1'),
    (130, '3.9.0'),
    (131, '3.9.1'),
    (132, '3.9.2'),
    (133, '3.9.3'),
    (134, '3.9.4'),
    (135, '3.9.5'),
    (136, '3.9.6'),
    (137, '3.9.7'),
    (138, '3.9.8'),
    (139, '3.9.9'),
    (140, '4.0.0');

-- Remember to update src/lib.rs and schema/inodes.sql after adding an exastash version
