-- Copyright 2004-2026 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select sum(cast(x as int)) from system_range(2147483547, 2147483637);
>> 195421006872

select sum(x) from system_range(9223372036854775707, 9223372036854775797);
>> 839326855353784593432

select sum(cast(100 as tinyint)) from system_range(1, 1000);
>> 100000

select sum(cast(100 as smallint)) from system_range(1, 1000);
>> 100000

-- with filter condition

create table test(v int);
> ok

insert into test values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12);
> update count: 12

select sum(v), sum(v) filter (where v >= 4) from test where v <= 10;
> SUM(V) SUM(V) FILTER (WHERE V >= 4)
> ------ ----------------------------
> 55     49
> rows: 1

create index test_idx on test(v);
> ok

select sum(v), sum(v) filter (where v >= 4) from test where v <= 10;
> SUM(V) SUM(V) FILTER (WHERE V >= 4)
> ------ ----------------------------
> 55     49
> rows: 1

insert into test values (1), (2), (8);
> update count: 3

select sum(v), sum(all v), sum(distinct v) from test;
> SUM(V) SUM(V) SUM(DISTINCT V)
> ------ ------ ---------------
> 89     89     78
> rows: 1

drop table test;
> ok

create table test(v interval day to second);
> ok

insert into test values ('0 1'), ('0 2'), ('0 2'), ('0 2'), ('-0 1'), ('-0 1');
> update count: 6

select sum(v) from test;
>> INTERVAL '0 05:00:00' DAY TO SECOND

drop table test;
> ok

SELECT X, COUNT(*), SUM(COUNT(*)) OVER() FROM VALUES (1), (1), (1), (1), (2), (2), (3) T(X) GROUP BY X;
> X COUNT(*) SUM(COUNT(*)) OVER ()
> - -------- ---------------------
> 1 4        7
> 2 2        7
> 3 1        7
> rows: 3

CREATE TABLE TEST(ID INT);
> ok

SELECT SUM(ID) FROM TEST;
>> null

SELECT SUM(ID) OVER () FROM TEST;
> SUM(ID) OVER ()
> ---------------
> rows: 0

DROP TABLE TEST;
> ok

SELECT
    ID,
    SUM(ID) OVER (ORDER BY ID) S,
    SUM(ID) OVER (ORDER BY ID RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) S_U_C,
    SUM(ID) OVER (ORDER BY ID RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) S_C_U,
    SUM(ID) OVER (ORDER BY ID RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) S_U_U
    FROM (SELECT X ID FROM SYSTEM_RANGE(1, 8));
> ID S  S_U_C S_C_U S_U_U
> -- -- ----- ----- -----
> 1  1  1     36    36
> 2  3  3     35    36
> 3  6  6     33    36
> 4  10 10    30    36
> 5  15 15    26    36
> 6  21 21    21    36
> 7  28 28    15    36
> 8  36 36    8     36
> rows: 8

SELECT I, V, SUM(V) OVER W S, SUM(DISTINCT V) OVER W D FROM
    VALUES (1, 1), (2, 1), (3, 1), (4, 1), (5, 2), (6, 2), (7, 3) T(I, V)
    WINDOW W AS (ORDER BY I);
> I V S  D
> - - -- -
> 1 1 1  1
> 2 1 2  1
> 3 1 3  1
> 4 1 4  1
> 5 2 6  3
> 6 2 8  3
> 7 3 11 6
> rows: 7

SELECT * FROM (SELECT SUM(V) OVER (ORDER BY V ROWS BETWEEN CURRENT ROW AND CURRENT ROW) S FROM (VALUES 1, 2, 2) T(V));
> S
> -
> 1
> 2
> 2
> rows: 3

SELECT V, SUM(V) FILTER (WHERE V <> 1) OVER (ROWS CURRENT ROW) S FROM (VALUES 1, 2, 2) T(V);
> V S
> - ----
> 1 null
> 2 2
> 2 2
> rows: 3

SELECT V,
    SUM(V) FILTER (WHERE V <> 1) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) S,
    SUM(V) FILTER (WHERE V <> 1) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) T
    FROM (VALUES 1, 2, 2) T(V);
> V S T
> - - -
> 1 4 2
> 2 4 4
> 2 4 4
> rows: 3



CREATE TABLE S(
    B BOOLEAN,
    N1 TINYINT,
    N2 SMALLINT,
    N4 INTEGER,
    N8 BIGINT,
    N NUMERIC(10, 2),
    F4 REAL,
    F8 DOUBLE PRECISION,
    D DECFLOAT(10),
    I1 INTERVAL YEAR(3),
    I2 INTERVAL MONTH(3),
    I3 INTERVAL DAY(3),
    I4 INTERVAL HOUR(3),
    I5 INTERVAL MINUTE(3),
    I6 INTERVAL SECOND(2),
    I7 INTERVAL YEAR(3) TO MONTH,
    I8 INTERVAL DAY(3) TO HOUR,
    I9 INTERVAL DAY(3) TO MINUTE,
    I10 INTERVAL DAY(3) TO SECOND(2),
    I11 INTERVAL HOUR(3) TO MINUTE,
    I12 INTERVAL HOUR(3) TO SECOND(2),
    I13 INTERVAL MINUTE(3) TO SECOND(2));
> ok

CREATE TABLE A AS SELECT
    SUM(B) B,
    SUM(N1) N1,
    SUM(N2) N2,
    SUM(N4) N4,
    SUM(N8) N8,
    SUM(N) N,
    SUM(F4) F4,
    SUM(F8) F8,
    SUM(D) D,
    SUM(I1) I1,
    SUM(I2) I2,
    SUM(I3) I3,
    SUM(I4) I4,
    SUM(I5) I5,
    SUM(I6) I6,
    SUM(I7) I7,
    SUM(I8) I8,
    SUM(I9) I9,
    SUM(I10) I10,
    SUM(I11) I11,
    SUM(I12) I12,
    SUM(I13) I13
    FROM S;
> ok

SELECT COLUMN_NAME, DATA_TYPE_SQL('PUBLIC', 'A', 'TABLE', DTD_IDENTIFIER) TYPE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'A' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME TYPE
> ----------- --------------------------------
> B           BIGINT
> N1          BIGINT
> N2          BIGINT
> N4          BIGINT
> N8          NUMERIC(29)
> N           NUMERIC(20, 2)
> F4          DOUBLE PRECISION
> F8          DECFLOAT(27)
> D           DECFLOAT(20)
> I1          INTERVAL YEAR(18)
> I2          INTERVAL MONTH(18)
> I3          INTERVAL DAY(18)
> I4          INTERVAL HOUR(18)
> I5          INTERVAL MINUTE(18)
> I6          INTERVAL SECOND(18)
> I7          INTERVAL YEAR(18) TO MONTH
> I8          INTERVAL DAY(18) TO HOUR
> I9          INTERVAL DAY(18) TO MINUTE
> I10         INTERVAL DAY(18) TO SECOND(2)
> I11         INTERVAL HOUR(18) TO MINUTE
> I12         INTERVAL HOUR(18) TO SECOND(2)
> I13         INTERVAL MINUTE(18) TO SECOND(2)
> rows (ordered): 22

DROP TABLE S, A;
> ok

SELECT SUM(I) FROM (VALUES INTERVAL '999999999999999999' SECOND, INTERVAL '1' SECOND) T(I);
> exception NUMERIC_VALUE_OUT_OF_RANGE_1

CREATE TABLE SALES(REGION VARCHAR(10), AMOUNT DECIMAL(12, 2));
> ok

INSERT INTO SALES VALUES ('EU', 100.50), ('US', 20.00);
> update count: 2

SELECT REGION, SUM(AMOUNT) REVENUE, SUM(-CAST(NULL AS DECIMAL(12, 2))) REFUNDS FROM SALES GROUP BY REGION;
> REGION REVENUE REFUNDS
> ------ ------- -------
> EU     100.50  null
> US     20.00   null
> rows: 2

SELECT SUM(0 - CAST(NULL AS DECIMAL(12, 2))) A, SUM(CAST(NULL AS DECIMAL(12, 2)) + 1) B,
    SUM(CAST(NULL AS DECIMAL(12, 2)) * 2) C, AVG(-CAST(NULL AS DECIMAL(12, 2))) D,
    SUM(-ABS(CAST(NULL AS DECIMAL(12, 2)))) E;
> A    B    C    D    E
> ---- ---- ---- ---- ----
> null null null null null
> rows: 1

CREATE TABLE REPORT AS SELECT REGION, -CAST(NULL AS DECIMAL(12, 2)) REFUNDS FROM SALES;
> ok

SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'REPORT' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE         NUMERIC_PRECISION NUMERIC_SCALE
> ----------- ----------------- ----------------- -------------
> REGION      CHARACTER VARYING null              null
> REFUNDS     NUMERIC           12                2
> rows (ordered): 2

DROP TABLE REPORT, SALES;
> ok

SELECT SUM(CASE 1 WHEN 2 THEN 3 END) A, SUM(CASE WHEN 1 = 2 THEN 3 END) B,
    SUM(CASE 1 WHEN 2 THEN 3 ELSE NULL END) C, AVG(CASE WHEN 1 = 2 THEN 3 ELSE NULL END) D;
> A    B    C    D
> ---- ---- ---- ----
> null null null null
> rows: 1

CREATE TABLE REPORT AS SELECT CASE 1 WHEN 2 THEN CAST(1 AS DECIMAL(5, 2)) END A,
    CASE WHEN 1 = 2 THEN CAST(1 AS DECIMAL(5, 2)) ELSE NULL END B;
> ok

SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'REPORT' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE NUMERIC_PRECISION NUMERIC_SCALE
> ----------- --------- ----------------- -------------
> A           NUMERIC   5                 2
> B           NUMERIC   5                 2
> rows (ordered): 2

DROP TABLE REPORT;
> ok
