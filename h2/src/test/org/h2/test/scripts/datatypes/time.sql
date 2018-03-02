-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(T1 TIME, T2 TIME WITHOUT TIME ZONE);
> ok

INSERT INTO TEST(T1, T2) VALUES (TIME '10:00:00', TIME WITHOUT TIME ZONE '10:00:00');
> update count: 1

SELECT T1, T2, T1 = T2 FROM TEST;
> T1       T2       T1 = T2
> -------- -------- -------
> 10:00:00 10:00:00 TRUE
> rows: 1

SELECT COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE TYPE_NAME COLUMN_TYPE
> ----------- --------- --------- ----------------------
> T1          92        TIME      TIME
> T2          92        TIME      TIME WITHOUT TIME ZONE
> rows (ordered): 2

DROP TABLE TEST;
> ok

-- Check that TIME is allowed as a column name
CREATE TABLE TEST(TIME TIME);
> ok

INSERT INTO TEST VALUES (TIME '08:00:00');
> update count: 1

SELECT TIME FROM TEST;
> TIME
> --------
> 08:00:00
> rows: 1

DROP TABLE TEST;
> ok
