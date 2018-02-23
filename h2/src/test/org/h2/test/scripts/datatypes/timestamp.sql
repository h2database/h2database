-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(T1 TIMESTAMP, T2 TIMESTAMP WITHOUT TIME ZONE);
> ok

INSERT INTO TEST(T1, T2) VALUES (TIMESTAMP '2010-01-01 10:00:00', TIMESTAMP WITHOUT TIME ZONE '2010-01-01 10:00:00');
> update count: 1

SELECT T1, T2, T1 = T2 FROM TEST;
> T1                    T2                    T1 = T2
> --------------------- --------------------- -------
> 2010-01-01 10:00:00.0 2010-01-01 10:00:00.0 TRUE
> rows: 1

SELECT COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE TYPE_NAME COLUMN_TYPE
> ----------- --------- --------- ---------------------------
> T1          93        TIMESTAMP TIMESTAMP
> T2          93        TIMESTAMP TIMESTAMP WITHOUT TIME ZONE
> rows (ordered): 2

DROP TABLE TEST;
> ok

-- Check that TIMESTAMP is allowed as a column name
CREATE TABLE TEST(TIMESTAMP TIMESTAMP);
> ok

INSERT INTO TEST VALUES (TIMESTAMP '1999-12-31 08:00:00');
> update count: 1

SELECT TIMESTAMP FROM TEST;
> TIMESTAMP
> ---------------------
> 1999-12-31 08:00:00.0
> rows: 1

DROP TABLE TEST;
> ok
