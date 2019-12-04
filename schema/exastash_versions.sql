CREATE TABLE exastash_versions (
    version_id   smallint  PRIMARY KEY,
    version_info text      UNIQUE NOT NULL CHECK (version_info ~ '\A.{3,64}\Z')
);

INSERT INTO exastash_versions (version_id, version_info) VALUES
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
    (41, '2.0.0');