-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select avg(cast(x as int)) from system_range(2147483547, 2147483637);
>> 2.147483592E9

select avg(x) from system_range(9223372036854775707, 9223372036854775797);
>> 9223372036854775752.0000000000

select avg(cast(100 as tinyint)) from system_range(1, 1000);
>> 100.0

select avg(cast(100 as smallint)) from system_range(1, 1000);
>> 100.0

-- with filter condition

create table test(v int);
> ok

insert into test values (10), (20), (30), (40), (50), (60), (70), (80), (90), (100), (110), (120);
> update count: 12

select avg(v), avg(v) filter (where v >= 40) from test where v <= 100;
> AVG(V) AVG(V) FILTER (WHERE V >= 40)
> ------ -----------------------------
> 55.0   70.0
> rows: 1

create index test_idx on test(v);
> ok

select avg(v), avg(v) filter (where v >= 40) from test where v <= 100;
> AVG(V) AVG(V) FILTER (WHERE V >= 40)
> ------ -----------------------------
> 55.0   70.0
> rows: 1

drop table test;
> ok

CREATE TABLE S(
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
    AVG(N1) N1,
    AVG(N2) N2,
    AVG(N4) N4,
    AVG(N8) N8,
    AVG(N) N,
    AVG(F4) F4,
    AVG(F8) F8,
    AVG(D) D,
    AVG(I1) I1,
    AVG(I2) I2,
    AVG(I3) I3,
    AVG(I4) I4,
    AVG(I5) I5,
    AVG(I6) I6,
    AVG(I7) I7,
    AVG(I8) I8,
    AVG(I9) I9,
    AVG(I10) I10,
    AVG(I11) I11,
    AVG(I12) I12,
    AVG(I13) I13
    FROM S;
> ok

SELECT COLUMN_NAME, DATA_TYPE_SQL('PUBLIC', 'A', 'TABLE', DTD_IDENTIFIER) TYPE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'A' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME TYPE
> ----------- -------------------------------
> N1          DOUBLE PRECISION
> N2          DOUBLE PRECISION
> N4          DOUBLE PRECISION
> N8          NUMERIC(29, 10)
> N           NUMERIC(20, 12)
> F4          DOUBLE PRECISION
> F8          DECFLOAT(27)
> D           DECFLOAT(20)
> I1          INTERVAL YEAR(3) TO MONTH
> I2          INTERVAL MONTH(3)
> I3          INTERVAL DAY(3) TO SECOND(9)
> I4          INTERVAL HOUR(3) TO SECOND(9)
> I5          INTERVAL MINUTE(3) TO SECOND(9)
> I6          INTERVAL SECOND(2, 9)
> I7          INTERVAL YEAR(3) TO MONTH
> I8          INTERVAL DAY(3) TO SECOND(9)
> I9          INTERVAL DAY(3) TO SECOND(9)
> I10         INTERVAL DAY(3) TO SECOND(9)
> I11         INTERVAL HOUR(3) TO SECOND(9)
> I12         INTERVAL HOUR(3) TO SECOND(9)
> I13         INTERVAL MINUTE(3) TO SECOND(9)
> rows (ordered): 21

DROP TABLE S, A;
> ok

SELECT AVG(X) FROM (VALUES INTERVAL '1' DAY, INTERVAL '2' DAY) T(X);
>> INTERVAL '1 12:00:00' DAY TO SECOND

SELECT AVG(X) FROM (VALUES CAST(1 AS NUMERIC(1)), CAST(2 AS NUMERIC(1))) T(X);
>> 1.5000000000

SELECT AVG(I) FROM (VALUES 9e99999 - 1, 1e99999 + 1) T(I);
>> 5E+99999

SELECT AVG(I) = 5E99999 FROM (VALUES CAST(9e99999 - 1 AS NUMERIC(100000)), CAST(1e99999 + 1 AS NUMERIC(100000))) T(I);
>> TRUE

SELECT AVG(I) FROM (VALUES INTERVAL '999999999999999999' SECOND, INTERVAL '1' SECOND) T(I);
>> INTERVAL '500000000000000000' SECOND
