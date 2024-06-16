-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- Test all possible order modes

CREATE TABLE TEST(A INT);
> ok

INSERT INTO TEST VALUES (NULL), (0), (1);
> update count: 3

-- default

SELECT A FROM TEST ORDER BY A;
> A
> ----
> null
> 0
> 1
> rows (ordered): 3

CREATE INDEX A_IDX ON TEST(A);
> ok

SELECT A FROM TEST ORDER BY A;
> A
> ----
> null
> 0
> 1
> rows (ordered): 3

EXPLAIN SELECT A FROM TEST ORDER BY A;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX */ ORDER BY 1 /* index sorted */

DROP INDEX A_IDX;
> ok

-- ASC

SELECT A FROM TEST ORDER BY A ASC;
> A
> ----
> null
> 0
> 1
> rows (ordered): 3

CREATE INDEX A_IDX ON TEST(A ASC);
> ok

SELECT A FROM TEST ORDER BY A ASC;
> A
> ----
> null
> 0
> 1
> rows (ordered): 3

EXPLAIN SELECT A FROM TEST ORDER BY A ASC;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX */ ORDER BY 1 /* index sorted */

DROP INDEX A_IDX;
> ok

-- ASC NULLS FIRST

SELECT A FROM TEST ORDER BY A ASC NULLS FIRST;
> A
> ----
> null
> 0
> 1
> rows (ordered): 3

CREATE INDEX A_IDX ON TEST(A ASC NULLS FIRST);
> ok

SELECT A FROM TEST ORDER BY A ASC NULLS FIRST;
> A
> ----
> null
> 0
> 1
> rows (ordered): 3

EXPLAIN SELECT A FROM TEST ORDER BY A ASC NULLS FIRST;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX */ ORDER BY 1 NULLS FIRST /* index sorted */

DROP INDEX A_IDX;
> ok

-- ASC NULLS LAST

SELECT A FROM TEST ORDER BY A ASC NULLS LAST;
> A
> ----
> 0
> 1
> null
> rows (ordered): 3

CREATE INDEX A_IDX ON TEST(A ASC NULLS LAST);
> ok

SELECT A FROM TEST ORDER BY A ASC NULLS LAST;
> A
> ----
> 0
> 1
> null
> rows (ordered): 3

EXPLAIN SELECT A FROM TEST ORDER BY A ASC NULLS LAST;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX */ ORDER BY 1 NULLS LAST /* index sorted */

DROP INDEX A_IDX;
> ok

-- DESC

SELECT A FROM TEST ORDER BY A DESC;
> A
> ----
> 1
> 0
> null
> rows (ordered): 3

CREATE INDEX A_IDX ON TEST(A DESC);
> ok

SELECT A FROM TEST ORDER BY A DESC;
> A
> ----
> 1
> 0
> null
> rows (ordered): 3

EXPLAIN SELECT A FROM TEST ORDER BY A DESC;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX */ ORDER BY 1 DESC /* index sorted */

DROP INDEX A_IDX;
> ok

-- DESC NULLS FIRST

SELECT A FROM TEST ORDER BY A DESC NULLS FIRST;
> A
> ----
> null
> 1
> 0
> rows (ordered): 3

CREATE INDEX A_IDX ON TEST(A DESC NULLS FIRST);
> ok

SELECT A FROM TEST ORDER BY A DESC NULLS FIRST;
> A
> ----
> null
> 1
> 0
> rows (ordered): 3

EXPLAIN SELECT A FROM TEST ORDER BY A DESC NULLS FIRST;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX */ ORDER BY 1 DESC NULLS FIRST /* index sorted */

DROP INDEX A_IDX;
> ok

-- DESC NULLS LAST

SELECT A FROM TEST ORDER BY A DESC NULLS LAST;
> A
> ----
> 1
> 0
> null
> rows (ordered): 3

CREATE INDEX A_IDX ON TEST(A DESC NULLS LAST);
> ok

SELECT A FROM TEST ORDER BY A DESC NULLS LAST;
> A
> ----
> 1
> 0
> null
> rows (ordered): 3

EXPLAIN SELECT A FROM TEST ORDER BY A DESC NULLS LAST;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX */ ORDER BY 1 DESC NULLS LAST /* index sorted */

DROP INDEX A_IDX;
> ok

-- Index selection

CREATE INDEX A_IDX_ASC ON TEST(A ASC);
> ok

CREATE INDEX A_IDX_ASC_NL ON TEST(A ASC NULLS LAST);
> ok

EXPLAIN SELECT A FROM TEST ORDER BY A;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX_ASC */ ORDER BY 1 /* index sorted */

EXPLAIN SELECT A FROM TEST ORDER BY A ASC;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX_ASC */ ORDER BY 1 /* index sorted */

EXPLAIN SELECT A FROM TEST ORDER BY A NULLS FIRST;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX_ASC */ ORDER BY 1 NULLS FIRST /* index sorted */

EXPLAIN SELECT A FROM TEST ORDER BY A NULLS LAST;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX_ASC_NL */ ORDER BY 1 NULLS LAST /* index sorted */

EXPLAIN SELECT A FROM TEST ORDER BY A DESC;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX_ASC */ ORDER BY 1 DESC /* index sorted */

EXPLAIN SELECT A FROM TEST ORDER BY A DESC NULLS FIRST;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX_ASC_NL */ ORDER BY 1 DESC NULLS FIRST /* index sorted */

EXPLAIN SELECT A FROM TEST ORDER BY A DESC NULLS LAST;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX_ASC */ ORDER BY 1 DESC NULLS LAST /* index sorted */

DROP INDEX A_IDX_ASC;
> ok

DROP INDEX A_IDX_ASC_NL;
> ok

CREATE INDEX A_IDX_DESC ON TEST(A DESC);
> ok

CREATE INDEX A_IDX_DESC_NF ON TEST(A DESC NULLS FIRST);
> ok

EXPLAIN SELECT A FROM TEST ORDER BY A;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX_DESC */ ORDER BY 1 /* index sorted */

EXPLAIN SELECT A FROM TEST ORDER BY A ASC;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX_DESC */ ORDER BY 1 /* index sorted */

EXPLAIN SELECT A FROM TEST ORDER BY A NULLS FIRST;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX_DESC */ ORDER BY 1 NULLS FIRST /* index sorted */

EXPLAIN SELECT A FROM TEST ORDER BY A NULLS LAST;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX_DESC_NF */ ORDER BY 1 NULLS LAST /* index sorted */

EXPLAIN SELECT A FROM TEST ORDER BY A DESC;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX_DESC */ ORDER BY 1 DESC /* index sorted */

EXPLAIN SELECT A FROM TEST ORDER BY A DESC NULLS FIRST;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX_DESC_NF */ ORDER BY 1 DESC NULLS FIRST /* index sorted */

EXPLAIN SELECT A FROM TEST ORDER BY A DESC NULLS LAST;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.A_IDX_DESC */ ORDER BY 1 DESC NULLS LAST /* index sorted */

DROP TABLE TEST;
> ok

-- Other tests

create table test(a int, b int);
> ok

insert into test values(1, 1);
> update count: 1

create index on test(a, b desc);
> ok

select * from test where a = 1;
> A B
> - -
> 1 1
> rows: 1

drop table test;
> ok

create table test(x int);
> ok

create hash index on test(x);
> ok

select 1 from test group by x;
> 1
> -
> rows: 0

drop table test;
> ok

CREATE TABLE TEST(A INT, B INT, C INT);
> ok

CREATE INDEX T_A1 ON TEST(A);
> ok

CREATE INDEX T_A_B ON TEST(A, B);
> ok

CREATE INDEX T_A_C ON TEST(A, C);
> ok

EXPLAIN SELECT * FROM TEST WHERE A = 0;
>> SELECT "PUBLIC"."TEST"."A", "PUBLIC"."TEST"."B", "PUBLIC"."TEST"."C" FROM "PUBLIC"."TEST" /* PUBLIC.T_A1: A = 0 */ WHERE "A" = 0

EXPLAIN SELECT * FROM TEST WHERE A = 0 AND B >= 0;
>> SELECT "PUBLIC"."TEST"."A", "PUBLIC"."TEST"."B", "PUBLIC"."TEST"."C" FROM "PUBLIC"."TEST" /* PUBLIC.T_A_B: A = 0 AND B >= 0 */ WHERE ("A" = 0) AND ("B" >= 0)

EXPLAIN SELECT * FROM TEST WHERE A > 0 AND B >= 0;
>> SELECT "PUBLIC"."TEST"."A", "PUBLIC"."TEST"."B", "PUBLIC"."TEST"."C" FROM "PUBLIC"."TEST" /* PUBLIC.T_A_B: A > 0 AND B >= 0 */ WHERE ("A" > 0) AND ("B" >= 0)

INSERT INTO TEST (SELECT X / 100, X, X FROM SYSTEM_RANGE(1, 3000));
> update count: 3000

EXPLAIN SELECT * FROM TEST WHERE A = 0;
>> SELECT "PUBLIC"."TEST"."A", "PUBLIC"."TEST"."B", "PUBLIC"."TEST"."C" FROM "PUBLIC"."TEST" /* PUBLIC.T_A1: A = 0 */ WHERE "A" = 0

EXPLAIN SELECT * FROM TEST WHERE A = 0 AND B >= 0;
>> SELECT "PUBLIC"."TEST"."A", "PUBLIC"."TEST"."B", "PUBLIC"."TEST"."C" FROM "PUBLIC"."TEST" /* PUBLIC.T_A_B: A = 0 AND B >= 0 */ WHERE ("A" = 0) AND ("B" >= 0)

EXPLAIN SELECT * FROM TEST WHERE A > 0 AND B >= 0;
>> SELECT "PUBLIC"."TEST"."A", "PUBLIC"."TEST"."B", "PUBLIC"."TEST"."C" FROM "PUBLIC"."TEST" /* PUBLIC.T_A_B: A > 0 AND B >= 0 */ WHERE ("A" > 0) AND ("B" >= 0)

-- Test that creation order of indexes has no effect
CREATE INDEX T_A2 ON TEST(A);
> ok

DROP INDEX T_A1;
> ok

EXPLAIN SELECT * FROM TEST WHERE A = 0;
>> SELECT "PUBLIC"."TEST"."A", "PUBLIC"."TEST"."B", "PUBLIC"."TEST"."C" FROM "PUBLIC"."TEST" /* PUBLIC.T_A2: A = 0 */ WHERE "A" = 0

DROP TABLE TEST;
> ok

CREATE TABLE T(A INT, B INT, C INT);
> ok

CREATE INDEX T_B_IDX ON T(B);
> ok

EXPLAIN SELECT * FROM T WHERE A = 1 AND B = A;
>> SELECT "PUBLIC"."T"."A", "PUBLIC"."T"."B", "PUBLIC"."T"."C" FROM "PUBLIC"."T" /* PUBLIC.T_B_IDX: B = 1 */ WHERE ("A" = 1) AND ("B" = "A")

DROP TABLE T;
> ok

-- _ROWID_ tests

CREATE TABLE TEST(ID INT PRIMARY KEY);
> ok

INSERT INTO TEST VALUES 1, 2, 3, 4;
> update count: 4

SELECT * FROM TEST WHERE ID >= 2 AND ID <= 3;
> ID
> --
> 2
> 3
> rows: 2

SELECT * FROM TEST WHERE _ROWID_ >= 2 AND _ROWID_ <= 3;
> ID
> --
> 2
> 3
> rows: 2

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID FLOAT PRIMARY KEY);
> ok

INSERT INTO TEST VALUES 1.0, 2.0, 3.0, 4.0;
> update count: 4

SELECT * FROM TEST WHERE ID >= 2.0 AND ID <= 3.0;
> ID
> ---
> 2.0
> 3.0
> rows: 2

SELECT * FROM TEST WHERE _ROWID_ >= 2 AND _ROWID_ <= 3;
> ID
> ---
> 2.0
> 3.0
> rows: 2

DROP TABLE TEST;
> ok

CREATE TABLE P AS SELECT 1 ID, GEOMETRY 'POLYGON ((160 280, 240 280, 240 140, 160 140, 160 280))' G;
> ok

CREATE INDEX ID_IDX ON P(ID);
> ok

CREATE SPATIAL INDEX P_G_INDEX ON P(G);
> ok

CREATE TABLE T AS SELECT 1 ID, 'A' K, 'A' V;
> ok

CREATE INDEX T_K_IDX ON T(K);
> ok

EXPLAIN SELECT P.ID, G, MAX(CASE WHEN K = 'A' THEN V END) AS A, MAX(CASE WHEN K = 'B' THEN V END) AS B
    FROM P JOIN T USING(ID)
    WHERE K IN ('A', 'C')
    AND G && GEOMETRY 'POLYGON ((198.5 186.5, 269.5 186.5, 269.5 115, 198.5 115, 198.5 186.5))'
    GROUP BY P.ID;
>> SELECT "P"."ID", "G", MAX(CASE WHEN "K" = 'A' THEN "V" END) AS "A", MAX(CASE WHEN "K" = 'B' THEN "V" END) AS "B" FROM "PUBLIC"."T" /* PUBLIC.T_K_IDX: K IN('A', 'C') */ /* WHERE K IN('A', 'C') */ INNER JOIN "PUBLIC"."P" /* PUBLIC.ID_IDX: ID = PUBLIC.T.ID */ ON 1=1 WHERE (("K" IN('A', 'C')) AND ("G" && GEOMETRY 'POLYGON ((198.5 186.5, 269.5 186.5, 269.5 115, 198.5 115, 198.5 186.5))')) AND ("PUBLIC"."P"."ID" = "PUBLIC"."T"."ID") GROUP BY "P"."ID"

DROP TABLE P, T;
> ok

CREATE TABLE TEST(A BIGINT PRIMARY KEY, B BIGINT UNIQUE);
> ok

INSERT INTO TEST VALUES (-9223372036854775808, -9223372036854775808), (0, 0),
    (9223372036854775807, 9223372036854775807);
> update count: 3

SELECT * FROM TEST WHERE A > 'NaN'::REAL;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B > 'NaN'::REAL;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A < 'NaN'::REAL;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE B < 'NaN'::REAL;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE A > 'Infinity'::REAL;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B > 'Infinity'::REAL;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A < 'Infinity'::REAL;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE B < 'Infinity'::REAL;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE A > 1E19::REAL;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B > 1E19::REAL;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A < 1E19::REAL;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE B < 1E19::REAL;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE A > '-Infinity'::REAL;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE B > '-Infinity'::REAL;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE A < '-Infinity'::REAL;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B < '-Infinity'::REAL;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A > -1E19::REAL;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE B > -1E19::REAL;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE A < -1E19::REAL;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B < -1E19::REAL;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A > 'NaN'::DOUBLE PRECISION;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B > 'NaN'::DOUBLE PRECISION;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A < 'NaN'::DOUBLE PRECISION;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE B < 'NaN'::DOUBLE PRECISION;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE A > 'Infinity'::DOUBLE PRECISION;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B > 'Infinity'::DOUBLE PRECISION;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A < 'Infinity'::DOUBLE PRECISION;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE B < 'Infinity'::DOUBLE PRECISION;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE A > 1E19::DOUBLE PRECISION;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B > 1E19::DOUBLE PRECISION;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A < 1E19::DOUBLE PRECISION;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE B < 1E19::DOUBLE PRECISION;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE A > '-Infinity'::DOUBLE PRECISION;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE B > '-Infinity'::DOUBLE PRECISION;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE A < '-Infinity'::DOUBLE PRECISION;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B < '-Infinity'::DOUBLE PRECISION;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A > -1E19::DOUBLE PRECISION;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE B > -1E19::DOUBLE PRECISION;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE A < -1E19::DOUBLE PRECISION;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B < -1E19::DOUBLE PRECISION;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A > 9223372036854775808::NUMERIC;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B > 9223372036854775808::NUMERIC;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A >= 9223372036854775807::NUMERIC;
> A                   B
> ------------------- -------------------
> 9223372036854775807 9223372036854775807
> rows: 1

SELECT * FROM TEST WHERE B >= 9223372036854775807::NUMERIC;
> A                   B
> ------------------- -------------------
> 9223372036854775807 9223372036854775807
> rows: 1

SELECT * FROM TEST WHERE A < 9223372036854775808::NUMERIC;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE B < 9223372036854775808::NUMERIC;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE A < 9223372036854775807::NUMERIC;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> rows: 2

SELECT * FROM TEST WHERE B < 9223372036854775807::NUMERIC;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> rows: 2

SELECT * FROM TEST WHERE A > -9223372036854775809::NUMERIC;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE B > -9223372036854775809::NUMERIC;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE A < -9223372036854775809::NUMERIC;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B < -9223372036854775809::NUMERIC;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A > 'NaN'::DECFLOAT;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B > 'NaN'::DECFLOAT;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A < 'NaN'::DECFLOAT;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE B < 'NaN'::DECFLOAT;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE A > 'Infinity'::DECFLOAT;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B > 'Infinity'::DECFLOAT;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A < 'Infinity'::DECFLOAT;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE B < 'Infinity'::DECFLOAT;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE A > 9223372036854775808::DECFLOAT;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B > 9223372036854775808::DECFLOAT;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A < 9223372036854775808::DECFLOAT;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE B < 9223372036854775808::DECFLOAT;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE A > '-Infinity'::DECFLOAT;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE B > '-Infinity'::DECFLOAT;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE A < '-Infinity'::DECFLOAT;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B < '-Infinity'::DECFLOAT;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A > -9223372036854775809::DECFLOAT;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE B > -9223372036854775809::DECFLOAT;
> A                    B
> -------------------- --------------------
> -9223372036854775808 -9223372036854775808
> 0                    0
> 9223372036854775807  9223372036854775807
> rows: 3

SELECT * FROM TEST WHERE A < -9223372036854775809::DECFLOAT;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B < -9223372036854775809::DECFLOAT;
> A B
> - -
> rows: 0

DROP TABLE TEST;
> ok

CREATE TABLE TEST(A TINYINT PRIMARY KEY, B TINYINT UNIQUE);
> ok

INSERT INTO TEST VALUES (-128, -128), (0, 0), (127, 127);
> update count: 3

SELECT * FROM TEST WHERE A > 'NaN'::REAL;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B > 'NaN'::REAL;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A < 'NaN'::REAL;
> A    B
> ---- ----
> -128 -128
> 0    0
> 127  127
> rows: 3

SELECT * FROM TEST WHERE B < 'NaN'::REAL;
> A    B
> ---- ----
> -128 -128
> 0    0
> 127  127
> rows: 3

SELECT * FROM TEST WHERE A > 'Infinity'::REAL;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B > 'Infinity'::REAL;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A < 'Infinity'::REAL;
> A    B
> ---- ----
> -128 -128
> 0    0
> 127  127
> rows: 3

SELECT * FROM TEST WHERE B < 'Infinity'::REAL;
> A    B
> ---- ----
> -128 -128
> 0    0
> 127  127
> rows: 3

SELECT * FROM TEST WHERE A > 1E19::REAL;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B > 1E19::REAL;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A < 1E19::REAL;
> A    B
> ---- ----
> -128 -128
> 0    0
> 127  127
> rows: 3

SELECT * FROM TEST WHERE B < 1E19::REAL;
> A    B
> ---- ----
> -128 -128
> 0    0
> 127  127
> rows: 3

SELECT * FROM TEST WHERE A > '-Infinity'::REAL;
> A    B
> ---- ----
> -128 -128
> 0    0
> 127  127
> rows: 3

SELECT * FROM TEST WHERE B > '-Infinity'::REAL;
> A    B
> ---- ----
> -128 -128
> 0    0
> 127  127
> rows: 3

SELECT * FROM TEST WHERE A < '-Infinity'::REAL;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B < '-Infinity'::REAL;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A > -1E19::REAL;
> A    B
> ---- ----
> -128 -128
> 0    0
> 127  127
> rows: 3

SELECT * FROM TEST WHERE B > -1E19::REAL;
> A    B
> ---- ----
> -128 -128
> 0    0
> 127  127
> rows: 3

SELECT * FROM TEST WHERE A < -1E19::REAL;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B < -1E19::REAL;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A > 'NaN'::DOUBLE PRECISION;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B > 'NaN'::DOUBLE PRECISION;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A < 'NaN'::DOUBLE PRECISION;
> A    B
> ---- ----
> -128 -128
> 0    0
> 127  127
> rows: 3

SELECT * FROM TEST WHERE B < 'NaN'::DOUBLE PRECISION;
> A    B
> ---- ----
> -128 -128
> 0    0
> 127  127
> rows: 3

SELECT * FROM TEST WHERE A > 'Infinity'::DOUBLE PRECISION;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B > 'Infinity'::DOUBLE PRECISION;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A < 'Infinity'::DOUBLE PRECISION;
> A    B
> ---- ----
> -128 -128
> 0    0
> 127  127
> rows: 3

SELECT * FROM TEST WHERE B < 'Infinity'::DOUBLE PRECISION;
> A    B
> ---- ----
> -128 -128
> 0    0
> 127  127
> rows: 3

SELECT * FROM TEST WHERE A > 1E19::DOUBLE PRECISION;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B > 1E19::DOUBLE PRECISION;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A < 1E19::DOUBLE PRECISION;
> A    B
> ---- ----
> -128 -128
> 0    0
> 127  127
> rows: 3

SELECT * FROM TEST WHERE B < 1E19::DOUBLE PRECISION;
> A    B
> ---- ----
> -128 -128
> 0    0
> 127  127
> rows: 3

SELECT * FROM TEST WHERE A > '-Infinity'::DOUBLE PRECISION;
> A    B
> ---- ----
> -128 -128
> 0    0
> 127  127
> rows: 3

SELECT * FROM TEST WHERE B > '-Infinity'::DOUBLE PRECISION;
> A    B
> ---- ----
> -128 -128
> 0    0
> 127  127
> rows: 3

SELECT * FROM TEST WHERE A < '-Infinity'::DOUBLE PRECISION;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B < '-Infinity'::DOUBLE PRECISION;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE A > -1E19::DOUBLE PRECISION;
> A    B
> ---- ----
> -128 -128
> 0    0
> 127  127
> rows: 3

SELECT * FROM TEST WHERE B > -1E19::DOUBLE PRECISION;
> A    B
> ---- ----
> -128 -128
> 0    0
> 127  127
> rows: 3

SELECT * FROM TEST WHERE A < -1E19::DOUBLE PRECISION;
> A B
> - -
> rows: 0

SELECT * FROM TEST WHERE B < -1E19::DOUBLE PRECISION;
> A B
> - -
> rows: 0

DROP TABLE TEST;
> ok

CREATE TABLE TEST(G GEOMETRY) AS VALUES GEOMETRY 'POINT(3 0)', GEOMETRY 'POINT(1 1)', GEOMETRY 'POINT(2 2)';
> ok

SELECT * FROM TEST ORDER BY G;
> G
> -----------
> POINT (1 1)
> POINT (2 2)
> POINT (3 0)
> rows (ordered): 3

SELECT * FROM TEST ORDER BY G DESC;
> G
> -----------
> POINT (3 0)
> POINT (2 2)
> POINT (1 1)
> rows (ordered): 3

CREATE SPATIAL INDEX ON TEST(G);
> ok

EXPLAIN SELECT * FROM TEST ORDER BY G;
>> SELECT "PUBLIC"."TEST"."G" FROM "PUBLIC"."TEST" /* PUBLIC.INDEX_2 */ ORDER BY 1 /* index sorted */

EXPLAIN SELECT * FROM TEST ORDER BY G DESC;
>> SELECT "PUBLIC"."TEST"."G" FROM "PUBLIC"."TEST" /* PUBLIC.INDEX_2 */ ORDER BY 1 DESC /* index sorted */

SELECT * FROM TEST ORDER BY G;
> G
> -----------
> POINT (1 1)
> POINT (2 2)
> POINT (3 0)
> rows (ordered): 3

SELECT * FROM TEST ORDER BY G DESC;
> G
> -----------
> POINT (3 0)
> POINT (2 2)
> POINT (1 1)
> rows (ordered): 3

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID INT);
> ok

INSERT INTO TEST VALUES 1, 2;
> update count: 2

SELECT _ROWID_, * FROM TEST ORDER BY _ROWID_ DESC;
> _ROWID_ ID
> ------- --
> 2       2
> 1       1
> rows (ordered): 2

DROP TABLE TEST;
> ok
