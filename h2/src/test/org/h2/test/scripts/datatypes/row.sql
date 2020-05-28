-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT ();
>> ROW ()

SELECT (1,);
> exception SYNTAX_ERROR_2

SELECT ROW ();
>> ROW ()

SELECT ROW (1,);
> exception SYNTAX_ERROR_2

SELECT ROW (10);
>> ROW (10)

SELECT (10, 20, 30);
>> ROW (10, 20, 30)

SELECT (1, NULL) IS NOT DISTINCT FROM (1, NULL);
>> TRUE

SELECT (1, NULL) IS DISTINCT FROM ROW (1, NULL);
>> FALSE

SELECT (1, NULL) = (1, NULL);
>> null

SELECT (1, NULL) <> (1, NULL);
>> null

SELECT ROW (NULL) = (NULL, NULL);
> exception COLUMN_COUNT_DOES_NOT_MATCH

select (1, NULL, 2) = (1, NULL, 1);
>> FALSE

select (1, NULL, 2) <> (1, NULL, 1);
>> TRUE

SELECT (1, NULL) > (1, NULL);
>> null

SELECT (1, 2) > (1, NULL);
>> null

SELECT (1, 2, NULL) > (1, 1, NULL);
>> TRUE

SELECT (1, 1, NULL) > (1, 2, NULL);
>> FALSE

SELECT (1, 2, NULL) < (1, 1, NULL);
>> FALSE

SELECT (1, 1, NULL) <= (1, 1, NULL);
>> null

SELECT (1, 2) IN (SELECT 1, 2);
>> TRUE

SELECT (1, 2) IN (SELECT * FROM VALUES (1, 2), (1, NULL));
>> TRUE

SELECT (1, 2) IN (SELECT * FROM VALUES (1, 1), (1, NULL));
>> null

SELECT (1, 2) IN (SELECT * FROM VALUES (1, 1), (1, 3));
>> FALSE

SELECT (1, NULL) IN (SELECT 1, NULL);
>> null

SELECT (1, ARRAY[1]) IN (SELECT 1, ARRAY[1]);
>> TRUE

SELECT (1, ARRAY[1]) IN (SELECT 1, ARRAY[2]);
>> FALSE

SELECT (1, ARRAY[NULL]) IN (SELECT 1, ARRAY[NULL]);
>> null

CREATE TABLE TEST (R ROW(A INT, B VARCHAR));
> ok

INSERT INTO TEST VALUES ((1, 2));
> update count: 1

INSERT INTO TEST VALUES ((1, X'3341'));
> update count: 1

TABLE TEST;
> R
> -----------
> ROW (1, 2)
> ROW (1, 3A)
> rows: 2

DROP TABLE TEST;
> ok

SELECT CAST((1, 2.1) AS ROW(A INT, B INT));
>> ROW (1, 2)

SELECT CAST((1, 2.1) AS ROW(A INT, B INT, C INT));
> exception DATA_CONVERSION_ERROR_1

SELECT CAST(1 AS ROW(V INT));
>> ROW (1)

SELECT CAST((1, 2) AS ROW(A INT, A INT));
> exception DUPLICATE_COLUMN_NAME_1

@reconnect off

CREATE LOCAL TEMPORARY TABLE TEST AS (SELECT ROW(1, 2) R);
> ok

CREATE INDEX IDX ON TEST(R);
> ok

DROP TABLE TEST;
> ok

CREATE LOCAL TEMPORARY TABLE TEST(R ROW(C CLOB));
> ok

CREATE INDEX IDX ON TEST(R);
> exception FEATURE_NOT_SUPPORTED_1

DROP TABLE TEST;
> ok

@reconnect on

-- The next tests should be at the of this file

SET MAX_MEMORY_ROWS = 2;
> ok

SELECT (X, X) FROM SYSTEM_RANGE(1, 100000) ORDER BY -X FETCH FIRST ROW ONLY;
>> ROW (100000, 100000)
