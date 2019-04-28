-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(ID INT);
> ok

INSERT INTO TEST VALUES 1, 2, 3, 4, 5, 6, 7, 8;
> update count: 8

DELETE FROM TEST WHERE EXISTS (SELECT X FROM SYSTEM_RANGE(1, 3) WHERE X = ID) AND ROWNUM() = 1;
> update count: 1

SELECT ID FROM TEST;
> ID
> --
> 2
> 3
> 4
> 5
> 6
> 7
> 8
> rows: 7

DELETE TEST WHERE ID = 2;
> exception SYNTAX_ERROR_2

SET MODE DB2;
> ok

DELETE TEST WHERE ID = 2;
> exception SYNTAX_ERROR_2

SET MODE Derby;
> ok

DELETE TEST WHERE ID = 2;
> exception SYNTAX_ERROR_2

SET MODE HSQLDB;
> ok

DELETE TEST WHERE ID = 2;
> exception SYNTAX_ERROR_2

SET MODE MySQL;
> ok

DELETE TEST WHERE ID = 2;
> exception SYNTAX_ERROR_2

DELETE TEST FROM TEST WHERE ID = 4;
> update count: 1

SET MODE PostgreSQL;
> ok

DELETE TEST WHERE ID = 2;
> exception SYNTAX_ERROR_2

SET MODE Ignite;
> ok

DELETE TEST WHERE ID = 2;
> exception SYNTAX_ERROR_2

SET MODE MSSQLServer;
> ok

DELETE TEST WHERE ID = 3;
> update count: 1

SET MODE Oracle;
> ok

DELETE TEST WHERE ID = 5;
> update count: 1

SET MODE Regular;
> ok

SELECT * FROM TEST;
> ID
> --
> 2
> 6
> 7
> 8
> rows: 4

DELETE TOP 1 FROM TEST;
> update count: 1

DELETE FROM TEST LIMIT 1;
> update count: 1

DELETE TOP 1 FROM TEST LIMIT 1;
> exception SYNTAX_ERROR_1

SELECT COUNT(*) FROM TEST;
>> 2

DROP TABLE TEST;
> ok
