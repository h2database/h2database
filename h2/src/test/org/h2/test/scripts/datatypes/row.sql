-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
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

CREATE DOMAIN D1 AS ROW(A INT);
> ok

CREATE DOMAIN D2 AS BIGINT ARRAY;
> ok

CREATE TABLE TEST(A ROW(A INT, B INT ARRAY[1]) ARRAY, B BIGINT ARRAY[2] ARRAY[3], C ROW(V BIGINT, A INT ARRAY),
    D D1, E D2);
> ok

SELECT COLUMN_NAME, DATA_TYPE, DOMAIN_NAME, MAXIMUM_CARDINALITY, DTD_IDENTIFIER FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'PUBLIC';
> COLUMN_NAME DATA_TYPE DOMAIN_NAME MAXIMUM_CARDINALITY DTD_IDENTIFIER
> ----------- --------- ----------- ------------------- --------------
> A           ARRAY     null        65536               1
> B           ARRAY     null        3                   2
> C           ROW       null        null                3
> D           ROW       D1          null                4
> E           ARRAY     D2          65536               5
> rows: 5

SELECT OBJECT_NAME, OBJECT_TYPE, COLLECTION_TYPE_IDENTIFIER, DATA_TYPE, MAXIMUM_CARDINALITY, DTD_IDENTIFIER
    FROM INFORMATION_SCHEMA.ELEMENT_TYPES;
> OBJECT_NAME OBJECT_TYPE COLLECTION_TYPE_IDENTIFIER DATA_TYPE MAXIMUM_CARDINALITY DTD_IDENTIFIER
> ----------- ----------- -------------------------- --------- ------------------- --------------
> D2          DOMAIN      TYPE                       BIGINT    null                TYPE_
> TEST        TABLE       1                          ROW       null                1_
> TEST        TABLE       1__2                       INTEGER   null                1__2_
> TEST        TABLE       2                          ARRAY     2                   2_
> TEST        TABLE       2_                         BIGINT    null                2__
> TEST        TABLE       3_2                        INTEGER   null                3_2_
> TEST        TABLE       5                          BIGINT    null                5_
> rows: 7

SELECT OBJECT_NAME, OBJECT_TYPE, ROW_IDENTIFIER, FIELD_NAME, ORDINAL_POSITION, DATA_TYPE, MAXIMUM_CARDINALITY,
    DTD_IDENTIFIER
    FROM INFORMATION_SCHEMA.FIELDS;
> OBJECT_NAME OBJECT_TYPE ROW_IDENTIFIER FIELD_NAME ORDINAL_POSITION DATA_TYPE MAXIMUM_CARDINALITY DTD_IDENTIFIER
> ----------- ----------- -------------- ---------- ---------------- --------- ------------------- --------------
> D1          DOMAIN      TYPE           A          1                INTEGER   null                TYPE_1
> TEST        TABLE       1_             A          1                INTEGER   null                1__1
> TEST        TABLE       1_             B          2                ARRAY     1                   1__2
> TEST        TABLE       3              A          2                ARRAY     65536               3_2
> TEST        TABLE       3              V          1                BIGINT    null                3_1
> TEST        TABLE       4              A          1                INTEGER   null                4_1
> rows: 6

DROP TABLE TEST;
> ok

DROP DOMAIN D1;
> ok

DROP DOMAIN D2;
> ok

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

EXECUTE IMMEDIATE 'CREATE TABLE TEST AS SELECT (' || (SELECT LISTAGG('1') FROM SYSTEM_RANGE(1, 16384)) || ')';
> ok

DROP TABLE TEST;
> ok

EXECUTE IMMEDIATE 'CREATE TABLE TEST AS SELECT (' || (SELECT LISTAGG('1') FROM SYSTEM_RANGE(1, 16385)) || ')';
> exception TOO_MANY_COLUMNS_1

EXECUTE IMMEDIATE 'CREATE TABLE TEST(R ROW(' || (SELECT LISTAGG('C' || X || ' INTEGER') FROM SYSTEM_RANGE(1, 16384)) || '))';
> ok

DROP TABLE TEST;
> ok

EXECUTE IMMEDIATE 'CREATE TABLE TEST(R ROW(' || (SELECT LISTAGG('C' || X || ' INTEGER') FROM SYSTEM_RANGE(1, 16385)) || '))';
> exception TOO_MANY_COLUMNS_1

-- The next tests should be at the of this file

SET MAX_MEMORY_ROWS = 2;
> ok

SELECT (X, X) FROM SYSTEM_RANGE(1, 100000) ORDER BY -X FETCH FIRST ROW ONLY;
>> ROW (100000, 100000)
