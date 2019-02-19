-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(A INT, B INT);
> ok

INSERT INTO TEST VALUES (1, 2);
> update count: 1

UPDATE TEST SET (A, B) = (3, 4);
> update count: 1

SELECT * FROM TEST;
> A B
> - -
> 3 4
> rows: 1

UPDATE TEST SET (B) = 5;
> update count: 1

SELECT B FROM TEST;
>> 5

UPDATE TEST SET (B) = ROW (6);
> update count: 1

SELECT B FROM TEST;
>> 6

UPDATE TEST SET (B) = (7);
> update count: 1

SELECT B FROM TEST;
>> 7

DROP TABLE TEST;
> ok
