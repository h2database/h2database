-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table orders ( orderid varchar(10), name varchar(20),  customer_id varchar(10),
    completed numeric(1) not null, verified numeric(1), a numeric(10, -3) );
> ok

SELECT COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION,
    NUMERIC_PRECISION_RADIX, NUMERIC_SCALE, COLUMN_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'ORDERS';
> COLUMN_NAME ORDINAL_POSITION IS_NULLABLE DATA_TYPE         CHARACTER_MAXIMUM_LENGTH NUMERIC_PRECISION NUMERIC_PRECISION_RADIX NUMERIC_SCALE COLUMN_TYPE
> ----------- ---------------- ----------- ----------------- ------------------------ ----------------- ----------------------- ------------- ---------------
> A           6                YES         NUMERIC           null                     10                10                      -3            NUMERIC(10, -3)
> COMPLETED   4                NO          NUMERIC           null                     1                 10                      0             NUMERIC(1)
> CUSTOMER_ID 3                YES         CHARACTER VARYING 10                       null              null                    null          VARCHAR(10)
> NAME        2                YES         CHARACTER VARYING 20                       null              null                    null          VARCHAR(20)
> ORDERID     1                YES         CHARACTER VARYING 10                       null              null                    null          VARCHAR(10)
> VERIFIED    5                YES         NUMERIC           null                     1                 10                      0             NUMERIC(1)
> rows: 6

drop table orders;
> ok

CREATE TABLE TEST(ID INT, X1 BIT, XT TINYINT, X_SM SMALLINT, XB BIGINT, XD DECIMAL(10,2), XD2 DOUBLE PRECISION, XR REAL);
> ok

INSERT INTO TEST VALUES(?, ?, ?, ?, ?, ?, ?, ?);
{
0,FALSE,0,0,0,0.0,0.0,0.0
1,TRUE,1,1,1,1.0,1.0,1.0
4,TRUE,4,4,4,4.0,4.0,4.0
-1,FALSE,-1,-1,-1,-1.0,-1.0,-1.0
NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
};
> update count: 5

SELECT ID, CAST(XT AS NUMBER(10,1)),
CAST(X_SM AS NUMBER(10,1)), CAST(XB AS NUMBER(10,1)), CAST(XD AS NUMBER(10,1)),
CAST(XD2 AS NUMBER(10,1)), CAST(XR AS NUMBER(10,1)) FROM TEST;
> ID   CAST(XT AS NUMERIC(10, 1)) CAST(X_SM AS NUMERIC(10, 1)) CAST(XB AS NUMERIC(10, 1)) CAST(XD AS NUMERIC(10, 1)) CAST(XD2 AS NUMERIC(10, 1)) CAST(XR AS NUMERIC(10, 1))
> ---- -------------------------- ---------------------------- -------------------------- -------------------------- --------------------------- --------------------------
> -1   -1.0                       -1.0                         -1.0                       -1.0                       -1.0                        -1.0
> 0    0.0                        0.0                          0.0                        0.0                        0.0                         0.0
> 1    1.0                        1.0                          1.0                        1.0                        1.0                         1.0
> 4    4.0                        4.0                          4.0                        4.0                        4.0                         4.0
> null null                       null                         null                       null                       null                        null
> rows: 5

DROP TABLE TEST;
> ok

CREATE TABLE TEST(I NUMERIC(-1));
> exception INVALID_VALUE_2

CREATE TABLE TEST(I NUMERIC(-1, -1));
> exception INVALID_VALUE_2

CREATE TABLE TEST (N NUMERIC(3, 1)) AS VALUES (0), (0.0), (NULL);
> ok

SELECT * FROM TEST;
> N
> ----
> 0.0
> 0.0
> null
> rows: 3

DROP TABLE TEST;
> ok

SELECT CAST(10000 AS NUMERIC(5, -3));
>> 1.0E+4

CREATE DOMAIN N AS NUMERIC(10, -1);
> ok

CREATE TABLE TEST(V N);
> ok

SELECT NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'V';
>> -1

DROP TABLE TEST;
> ok

DROP DOMAIN N;
> ok

CREATE TABLE TEST(I INT PRIMARY KEY, V NUMERIC(1, 3));
> ok

INSERT INTO TEST VALUES (1, 1e-3), (2, 1.1e-3), (3, 1e-4);
> update count: 3

INSERT INTO TEST VALUES (4, 1e-2);
> exception VALUE_TOO_LONG_2

TABLE TEST;
> I V
> - -----
> 1 0.001
> 2 0.001
> 3 0.000
> rows: 3

DROP TABLE TEST;
> ok

CREATE TABLE TEST(I INT PRIMARY KEY, V NUMERIC(2));
> ok

INSERT INTO TEST VALUES (1, 1e-1), (2, 2e0), (3, 3e1);
> update count: 3

TABLE TEST;
> I V
> - --
> 1 0
> 2 2
> 3 30
> rows: 3

DROP TABLE TEST;
> ok

EXPLAIN VALUES (CAST(-9223372036854775808 AS NUMERIC(19)), CAST(9223372036854775807 AS NUMERIC(19)), 1.0, -9223372036854775809,
    9223372036854775808);
>> VALUES (CAST(-9223372036854775808 AS NUMERIC(19)), CAST(9223372036854775807 AS NUMERIC(19)), 1.0, -9223372036854775809, 9223372036854775808)

CREATE TABLE T(C NUMERIC(0));
> exception INVALID_VALUE_2
