-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE DOMAIN E AS ENUM('A', 'B');
> ok

CREATE TABLE TEST(I INT PRIMARY KEY, E1 E, E2 E NOT NULL);
> ok

INSERT INTO TEST VALUES (1, 'A', 'B');
> update count: 1

SELECT COLUMN_NAME, NULLABLE, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME NULLABLE COLUMN_TYPE
> ----------- -------- ------------
> I           0        INT NOT NULL
> E1          1        E
> E2          0        E NOT NULL
> rows (ordered): 3

DROP DOMAIN E RESTRICT;
> exception CANNOT_DROP_2

DROP DOMAIN E CASCADE;
> ok

SELECT COLUMN_NAME, NULLABLE, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME NULLABLE COLUMN_TYPE
> ----------- -------- ----------------------
> I           0        INT NOT NULL
> E1          1        ENUM('A','B')
> E2          0        ENUM('A','B') NOT NULL
> rows (ordered): 3

DROP TABLE TEST;
> ok
