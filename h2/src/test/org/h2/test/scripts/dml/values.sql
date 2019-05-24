-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

VALUES (1, 2);
> C1 C2
> -- --
> 1  2
> rows: 1

VALUES ROW (1, 2);
> C1 C2
> -- --
> 1  2
> rows: 1

VALUES 1, 2;
> C1
> --
> 1
> 2
> rows: 2

VALUES 4, 3, 1, 2 ORDER BY 1 FETCH FIRST 75 PERCENT ROWS ONLY;
> C1
> --
> 1
> 2
> 3
> rows (ordered): 3

SELECT * FROM (VALUES (1::BIGINT, 2)) T (A, B) WHERE (A, B) IN (VALUES(1, 2));
> A B
> - -
> 1 2
> rows: 1

SELECT * FROM (VALUES (1000000000000, 2)) T (A, B) WHERE (A, B) IN (VALUES(1, 2));
> A B
> - -
> rows: 0

SELECT * FROM (VALUES (1, 2)) T (A, B) WHERE (A, B) IN (VALUES(1::BIGINT, 2));
> A B
> - -
> 1 2
> rows: 1

SELECT * FROM (VALUES (1, 2)) T (A, B) WHERE (A, B) IN (VALUES(1000000000000, 2));
> A B
> - -
> rows: 0

EXPLAIN VALUES 1, (2), ROW(3);
>> VALUES (1), (2), (3)

EXPLAIN VALUES (1, 2), (3, 4);
>> VALUES (1, 2), (3, 4)

EXPLAIN SELECT * FROM (VALUES 1, 2) T(V);
>> SELECT "T"."V" FROM (VALUES (1), (2)) "T"("V") /* table scan */

EXPLAIN SELECT * FROM (VALUES 1, 2);
>> SELECT "_5"."C1" FROM (VALUES (1), (2)) "_5" /* table scan */

EXPLAIN SELECT * FROM (VALUES 1, 2 ORDER BY 1 DESC);
>> SELECT "_6"."C1" FROM ( VALUES (1), (2) ORDER BY 1 DESC ) "_6" /* VALUES (1), (2) ORDER BY 1 DESC */

-- Non-standard syntax
EXPLAIN SELECT * FROM VALUES 1, 2;
>> SELECT "_7"."C1" FROM (VALUES (1), (2)) "_7" /* table scan */

VALUES (1, 2), (3, 4), (5, 1) ORDER BY C1 + C2;
> C1 C2
> -- --
> 1  2
> 5  1
> 3  4
> rows (ordered): 3

VALUES (1, 2), (3, 4), (5, 1) ORDER BY C1 + C2, C1 * C2;
> C1 C2
> -- --
> 1  2
> 5  1
> 3  4
> rows (ordered): 3

VALUES (1, 2), (3, 4), (5, 1) ORDER BY C1 + C2, C1 * C2 OFFSET 1 ROW FETCH FIRST 1 ROW ONLY;
> C1 C2
> -- --
> 5  1
> rows (ordered): 1

EXPLAIN VALUES (1, 2), (3, 4), (5, 1) ORDER BY C1 + C2, = C1 * C2 OFFSET 1 ROW FETCH FIRST 1 ROW ONLY;
>> VALUES (1, 2), (3, 4), (5, 1) ORDER BY ="C1" + "C2", ="C1" * "C2" OFFSET 1 ROW FETCH NEXT ROW ONLY
