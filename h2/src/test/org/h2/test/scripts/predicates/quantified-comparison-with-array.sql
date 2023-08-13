-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE T1 AS SELECT * FROM (VALUES 0, 1, 2, 3, NULL) T(V);
> ok

CREATE TABLE T2 AS SELECT * FROM (VALUES NULL, ARRAY[], ARRAY[NULL], ARRAY[1], ARRAY[1, NULL], ARRAY[1, 2], ARRAY[1, 2, NULL]) T(A);
> ok

SELECT V, A,
    V = ANY(A), NOT(V <> ALL(A)),
    V = ALL(A), NOT(V <> ANY(A)),
    V <> ANY(A), NOT(V = ALL(A)),
    V <> ALL(A), NOT(V = ANY(A))
    FROM T1, T2;
> V    A            V = ANY(A) V = ANY(A) V = ALL(A) V = ALL(A) V <> ANY(A) V <> ANY(A) V <> ALL(A) V <> ALL(A)
> ---- ------------ ---------- ---------- ---------- ---------- ----------- ----------- ----------- -----------
> 0    [1, 2, null] null       null       FALSE      FALSE      TRUE        TRUE        null        null
> 0    [1, 2]       FALSE      FALSE      FALSE      FALSE      TRUE        TRUE        TRUE        TRUE
> 0    [1, null]    null       null       FALSE      FALSE      TRUE        TRUE        null        null
> 0    [1]          FALSE      FALSE      FALSE      FALSE      TRUE        TRUE        TRUE        TRUE
> 0    []           FALSE      FALSE      TRUE       TRUE       FALSE       FALSE       TRUE        TRUE
> 0    [null]       null       null       null       null       null        null        null        null
> 0    null         null       null       null       null       null        null        null        null
> 1    [1, 2, null] TRUE       TRUE       FALSE      FALSE      TRUE        TRUE        FALSE       FALSE
> 1    [1, 2]       TRUE       TRUE       FALSE      FALSE      TRUE        TRUE        FALSE       FALSE
> 1    [1, null]    TRUE       TRUE       null       null       null        null        FALSE       FALSE
> 1    [1]          TRUE       TRUE       TRUE       TRUE       FALSE       FALSE       FALSE       FALSE
> 1    []           FALSE      FALSE      TRUE       TRUE       FALSE       FALSE       TRUE        TRUE
> 1    [null]       null       null       null       null       null        null        null        null
> 1    null         null       null       null       null       null        null        null        null
> 2    [1, 2, null] TRUE       TRUE       FALSE      FALSE      TRUE        TRUE        FALSE       FALSE
> 2    [1, 2]       TRUE       TRUE       FALSE      FALSE      TRUE        TRUE        FALSE       FALSE
> 2    [1, null]    null       null       FALSE      FALSE      TRUE        TRUE        null        null
> 2    [1]          FALSE      FALSE      FALSE      FALSE      TRUE        TRUE        TRUE        TRUE
> 2    []           FALSE      FALSE      TRUE       TRUE       FALSE       FALSE       TRUE        TRUE
> 2    [null]       null       null       null       null       null        null        null        null
> 2    null         null       null       null       null       null        null        null        null
> 3    [1, 2, null] null       null       FALSE      FALSE      TRUE        TRUE        null        null
> 3    [1, 2]       FALSE      FALSE      FALSE      FALSE      TRUE        TRUE        TRUE        TRUE
> 3    [1, null]    null       null       FALSE      FALSE      TRUE        TRUE        null        null
> 3    [1]          FALSE      FALSE      FALSE      FALSE      TRUE        TRUE        TRUE        TRUE
> 3    []           FALSE      FALSE      TRUE       TRUE       FALSE       FALSE       TRUE        TRUE
> 3    [null]       null       null       null       null       null        null        null        null
> 3    null         null       null       null       null       null        null        null        null
> null [1, 2, null] null       null       null       null       null        null        null        null
> null [1, 2]       null       null       null       null       null        null        null        null
> null [1, null]    null       null       null       null       null        null        null        null
> null [1]          null       null       null       null       null        null        null        null
> null []           FALSE      FALSE      TRUE       TRUE       FALSE       FALSE       TRUE        TRUE
> null [null]       null       null       null       null       null        null        null        null
> null null         null       null       null       null       null        null        null        null
> rows: 35

SELECT V, A,
    V IS NOT DISTINCT FROM ANY(A), NOT(V IS DISTINCT FROM ALL(A)),
    V IS NOT DISTINCT FROM ALL(A), NOT(V IS DISTINCT FROM ANY(A)),
    V IS DISTINCT FROM ANY(A), NOT(V IS NOT DISTINCT FROM ALL(A)),
    V IS DISTINCT FROM ALL(A), NOT(V IS NOT DISTINCT FROM ANY(A))
    FROM T1, T2;
> V    A            V IS NOT DISTINCT FROM ANY(A) V IS NOT DISTINCT FROM ANY(A) V IS NOT DISTINCT FROM ALL(A) V IS NOT DISTINCT FROM ALL(A) V IS DISTINCT FROM ANY(A) V IS DISTINCT FROM ANY(A) V IS DISTINCT FROM ALL(A) V IS DISTINCT FROM ALL(A)
> ---- ------------ ----------------------------- ----------------------------- ----------------------------- ----------------------------- ------------------------- ------------------------- ------------------------- -------------------------
> 0    [1, 2, null] FALSE                         FALSE                         FALSE                         FALSE                         TRUE                      TRUE                      TRUE                      TRUE
> 0    [1, 2]       FALSE                         FALSE                         FALSE                         FALSE                         TRUE                      TRUE                      TRUE                      TRUE
> 0    [1, null]    FALSE                         FALSE                         FALSE                         FALSE                         TRUE                      TRUE                      TRUE                      TRUE
> 0    [1]          FALSE                         FALSE                         FALSE                         FALSE                         TRUE                      TRUE                      TRUE                      TRUE
> 0    []           FALSE                         FALSE                         TRUE                          TRUE                          FALSE                     FALSE                     TRUE                      TRUE
> 0    [null]       FALSE                         FALSE                         FALSE                         FALSE                         TRUE                      TRUE                      TRUE                      TRUE
> 0    null         null                          null                          null                          null                          null                      null                      null                      null
> 1    [1, 2, null] TRUE                          TRUE                          FALSE                         FALSE                         TRUE                      TRUE                      FALSE                     FALSE
> 1    [1, 2]       TRUE                          TRUE                          FALSE                         FALSE                         TRUE                      TRUE                      FALSE                     FALSE
> 1    [1, null]    TRUE                          TRUE                          FALSE                         FALSE                         TRUE                      TRUE                      FALSE                     FALSE
> 1    [1]          TRUE                          TRUE                          TRUE                          TRUE                          FALSE                     FALSE                     FALSE                     FALSE
> 1    []           FALSE                         FALSE                         TRUE                          TRUE                          FALSE                     FALSE                     TRUE                      TRUE
> 1    [null]       FALSE                         FALSE                         FALSE                         FALSE                         TRUE                      TRUE                      TRUE                      TRUE
> 1    null         null                          null                          null                          null                          null                      null                      null                      null
> 2    [1, 2, null] TRUE                          TRUE                          FALSE                         FALSE                         TRUE                      TRUE                      FALSE                     FALSE
> 2    [1, 2]       TRUE                          TRUE                          FALSE                         FALSE                         TRUE                      TRUE                      FALSE                     FALSE
> 2    [1, null]    FALSE                         FALSE                         FALSE                         FALSE                         TRUE                      TRUE                      TRUE                      TRUE
> 2    [1]          FALSE                         FALSE                         FALSE                         FALSE                         TRUE                      TRUE                      TRUE                      TRUE
> 2    []           FALSE                         FALSE                         TRUE                          TRUE                          FALSE                     FALSE                     TRUE                      TRUE
> 2    [null]       FALSE                         FALSE                         FALSE                         FALSE                         TRUE                      TRUE                      TRUE                      TRUE
> 2    null         null                          null                          null                          null                          null                      null                      null                      null
> 3    [1, 2, null] FALSE                         FALSE                         FALSE                         FALSE                         TRUE                      TRUE                      TRUE                      TRUE
> 3    [1, 2]       FALSE                         FALSE                         FALSE                         FALSE                         TRUE                      TRUE                      TRUE                      TRUE
> 3    [1, null]    FALSE                         FALSE                         FALSE                         FALSE                         TRUE                      TRUE                      TRUE                      TRUE
> 3    [1]          FALSE                         FALSE                         FALSE                         FALSE                         TRUE                      TRUE                      TRUE                      TRUE
> 3    []           FALSE                         FALSE                         TRUE                          TRUE                          FALSE                     FALSE                     TRUE                      TRUE
> 3    [null]       FALSE                         FALSE                         FALSE                         FALSE                         TRUE                      TRUE                      TRUE                      TRUE
> 3    null         null                          null                          null                          null                          null                      null                      null                      null
> null [1, 2, null] TRUE                          TRUE                          FALSE                         FALSE                         TRUE                      TRUE                      FALSE                     FALSE
> null [1, 2]       FALSE                         FALSE                         FALSE                         FALSE                         TRUE                      TRUE                      TRUE                      TRUE
> null [1, null]    TRUE                          TRUE                          FALSE                         FALSE                         TRUE                      TRUE                      FALSE                     FALSE
> null [1]          FALSE                         FALSE                         FALSE                         FALSE                         TRUE                      TRUE                      TRUE                      TRUE
> null []           FALSE                         FALSE                         TRUE                          TRUE                          FALSE                     FALSE                     TRUE                      TRUE
> null [null]       TRUE                          TRUE                          TRUE                          TRUE                          FALSE                     FALSE                     FALSE                     FALSE
> null null         null                          null                          null                          null                          null                      null                      null                      null
> rows: 35

SELECT V, A,
    V >= ANY(A), NOT(V < ALL(A)),
    V >= ALL(A), NOT(V < ANY(A)),
    V < ANY(A), NOT(V >= ALL(A)),
    V < ALL(A), NOT(V >= ANY(A))
    FROM T1, T2;
> V    A            V >= ANY(A) V >= ANY(A) V >= ALL(A) V >= ALL(A) V < ANY(A) V < ANY(A) V < ALL(A) V < ALL(A)
> ---- ------------ ----------- ----------- ----------- ----------- ---------- ---------- ---------- ----------
> 0    [1, 2, null] null        null        FALSE       FALSE       TRUE       TRUE       null       null
> 0    [1, 2]       FALSE       FALSE       FALSE       FALSE       TRUE       TRUE       TRUE       TRUE
> 0    [1, null]    null        null        FALSE       FALSE       TRUE       TRUE       null       null
> 0    [1]          FALSE       FALSE       FALSE       FALSE       TRUE       TRUE       TRUE       TRUE
> 0    []           FALSE       FALSE       TRUE        TRUE        FALSE      FALSE      TRUE       TRUE
> 0    [null]       null        null        null        null        null       null       null       null
> 0    null         null        null        null        null        null       null       null       null
> 1    [1, 2, null] TRUE        TRUE        FALSE       FALSE       TRUE       TRUE       FALSE      FALSE
> 1    [1, 2]       TRUE        TRUE        FALSE       FALSE       TRUE       TRUE       FALSE      FALSE
> 1    [1, null]    TRUE        TRUE        null        null        null       null       FALSE      FALSE
> 1    [1]          TRUE        TRUE        TRUE        TRUE        FALSE      FALSE      FALSE      FALSE
> 1    []           FALSE       FALSE       TRUE        TRUE        FALSE      FALSE      TRUE       TRUE
> 1    [null]       null        null        null        null        null       null       null       null
> 1    null         null        null        null        null        null       null       null       null
> 2    [1, 2, null] TRUE        TRUE        null        null        null       null       FALSE      FALSE
> 2    [1, 2]       TRUE        TRUE        TRUE        TRUE        FALSE      FALSE      FALSE      FALSE
> 2    [1, null]    TRUE        TRUE        null        null        null       null       FALSE      FALSE
> 2    [1]          TRUE        TRUE        TRUE        TRUE        FALSE      FALSE      FALSE      FALSE
> 2    []           FALSE       FALSE       TRUE        TRUE        FALSE      FALSE      TRUE       TRUE
> 2    [null]       null        null        null        null        null       null       null       null
> 2    null         null        null        null        null        null       null       null       null
> 3    [1, 2, null] TRUE        TRUE        null        null        null       null       FALSE      FALSE
> 3    [1, 2]       TRUE        TRUE        TRUE        TRUE        FALSE      FALSE      FALSE      FALSE
> 3    [1, null]    TRUE        TRUE        null        null        null       null       FALSE      FALSE
> 3    [1]          TRUE        TRUE        TRUE        TRUE        FALSE      FALSE      FALSE      FALSE
> 3    []           FALSE       FALSE       TRUE        TRUE        FALSE      FALSE      TRUE       TRUE
> 3    [null]       null        null        null        null        null       null       null       null
> 3    null         null        null        null        null        null       null       null       null
> null [1, 2, null] null        null        null        null        null       null       null       null
> null [1, 2]       null        null        null        null        null       null       null       null
> null [1, null]    null        null        null        null        null       null       null       null
> null [1]          null        null        null        null        null       null       null       null
> null []           FALSE       FALSE       TRUE        TRUE        FALSE      FALSE      TRUE       TRUE
> null [null]       null        null        null        null        null       null       null       null
> null null         null        null        null        null        null       null       null       null
> rows: 35

SELECT V, A,
    V <= ANY(A), NOT(V > ALL(A)),
    V <= ALL(A), NOT(V > ANY(A)),
    V > ANY(A), NOT(V <= ALL(A)),
    V > ALL(A), NOT(V <= ANY(A))
    FROM T1, T2;
> V    A            V <= ANY(A) V <= ANY(A) V <= ALL(A) V <= ALL(A) V > ANY(A) V > ANY(A) V > ALL(A) V > ALL(A)
> ---- ------------ ----------- ----------- ----------- ----------- ---------- ---------- ---------- ----------
> 0    [1, 2, null] TRUE        TRUE        null        null        null       null       FALSE      FALSE
> 0    [1, 2]       TRUE        TRUE        TRUE        TRUE        FALSE      FALSE      FALSE      FALSE
> 0    [1, null]    TRUE        TRUE        null        null        null       null       FALSE      FALSE
> 0    [1]          TRUE        TRUE        TRUE        TRUE        FALSE      FALSE      FALSE      FALSE
> 0    []           FALSE       FALSE       TRUE        TRUE        FALSE      FALSE      TRUE       TRUE
> 0    [null]       null        null        null        null        null       null       null       null
> 0    null         null        null        null        null        null       null       null       null
> 1    [1, 2, null] TRUE        TRUE        null        null        null       null       FALSE      FALSE
> 1    [1, 2]       TRUE        TRUE        TRUE        TRUE        FALSE      FALSE      FALSE      FALSE
> 1    [1, null]    TRUE        TRUE        null        null        null       null       FALSE      FALSE
> 1    [1]          TRUE        TRUE        TRUE        TRUE        FALSE      FALSE      FALSE      FALSE
> 1    []           FALSE       FALSE       TRUE        TRUE        FALSE      FALSE      TRUE       TRUE
> 1    [null]       null        null        null        null        null       null       null       null
> 1    null         null        null        null        null        null       null       null       null
> 2    [1, 2, null] TRUE        TRUE        FALSE       FALSE       TRUE       TRUE       FALSE      FALSE
> 2    [1, 2]       TRUE        TRUE        FALSE       FALSE       TRUE       TRUE       FALSE      FALSE
> 2    [1, null]    null        null        FALSE       FALSE       TRUE       TRUE       null       null
> 2    [1]          FALSE       FALSE       FALSE       FALSE       TRUE       TRUE       TRUE       TRUE
> 2    []           FALSE       FALSE       TRUE        TRUE        FALSE      FALSE      TRUE       TRUE
> 2    [null]       null        null        null        null        null       null       null       null
> 2    null         null        null        null        null        null       null       null       null
> 3    [1, 2, null] null        null        FALSE       FALSE       TRUE       TRUE       null       null
> 3    [1, 2]       FALSE       FALSE       FALSE       FALSE       TRUE       TRUE       TRUE       TRUE
> 3    [1, null]    null        null        FALSE       FALSE       TRUE       TRUE       null       null
> 3    [1]          FALSE       FALSE       FALSE       FALSE       TRUE       TRUE       TRUE       TRUE
> 3    []           FALSE       FALSE       TRUE        TRUE        FALSE      FALSE      TRUE       TRUE
> 3    [null]       null        null        null        null        null       null       null       null
> 3    null         null        null        null        null        null       null       null       null
> null [1, 2, null] null        null        null        null        null       null       null       null
> null [1, 2]       null        null        null        null        null       null       null       null
> null [1, null]    null        null        null        null        null       null       null       null
> null [1]          null        null        null        null        null       null       null       null
> null []           FALSE       FALSE       TRUE        TRUE        FALSE      FALSE      TRUE       TRUE
> null [null]       null        null        null        null        null       null       null       null
> null null         null        null        null        null        null       null       null       null
> rows: 35

EXPLAIN SELECT * FROM T1 WHERE V = ANY(ARRAY[1, 2]);
>> SELECT "PUBLIC"."T1"."V" FROM "PUBLIC"."T1" /* PUBLIC.T1.tableScan */ WHERE "V" = ANY(ARRAY [1, 2])

SELECT * FROM T1 WHERE V = ANY(ARRAY[1, 2]);
> V
> -
> 1
> 2
> rows: 2

EXPLAIN SELECT V, A FROM T1 JOIN T2 ON T1.V = ANY(T2.A);
>> SELECT "V", "A" FROM "PUBLIC"."T1" /* PUBLIC.T1.tableScan */ INNER JOIN "PUBLIC"."T2" /* PUBLIC.T2.tableScan */ ON 1=1 WHERE "T1"."V" = ANY("T2"."A")

SELECT V, A FROM T1 JOIN T2 ON T1.V = ANY(T2.A);
> V A
> - ------------
> 1 [1, 2, null]
> 1 [1, 2]
> 1 [1, null]
> 1 [1]
> 2 [1, 2, null]
> 2 [1, 2]
> rows: 6

CREATE INDEX T1_V_IDX ON T1(V);
> ok

EXPLAIN SELECT * FROM T1 WHERE V = ANY(ARRAY[1, 3]);
>> SELECT "PUBLIC"."T1"."V" FROM "PUBLIC"."T1" /* PUBLIC.T1_V_IDX: V IN(1, 3) */ WHERE "V" = ANY(ARRAY [1, 3])

SELECT * FROM T1 WHERE V = ANY(ARRAY[1, 3]);
> V
> -
> 1
> 3
> rows: 2

EXPLAIN SELECT * FROM T1 WHERE V = ANY(ARRAY[]);
>> SELECT "PUBLIC"."T1"."V" FROM "PUBLIC"."T1" /* PUBLIC.T1.tableScan: FALSE */ WHERE "V" = ANY(ARRAY [])

SELECT * FROM T1 WHERE V = ANY(ARRAY[]);
> V
> -
> rows: 0

EXPLAIN SELECT V, A FROM T1 JOIN T2 ON T1.V = ANY(T2.A);
>> SELECT "V", "A" FROM "PUBLIC"."T2" /* PUBLIC.T2.tableScan */ INNER JOIN "PUBLIC"."T1" /* PUBLIC.T1_V_IDX: V = ANY(T2.A) */ ON 1=1 WHERE "T1"."V" = ANY("T2"."A")

SELECT V, A FROM T1 JOIN T2 ON T1.V = ANY(T2.A);
> V A
> - ------------
> 1 [1, 2, null]
> 1 [1, 2]
> 1 [1, null]
> 1 [1]
> 2 [1, 2, null]
> 2 [1, 2]
> rows: 6

EXPLAIN SELECT * FROM T1 WHERE T1.V = ANY(CAST((SELECT ARRAY_AGG(S.V) FROM T1 S) AS INTEGER ARRAY));
>> SELECT "PUBLIC"."T1"."V" FROM "PUBLIC"."T1" /* PUBLIC.T1_V_IDX: V = ANY(CAST((SELECT ARRAY_AGG(S.V) FROM PUBLIC.T1 S /* PUBLIC.T1_V_IDX */) AS INTEGER ARRAY)) */ WHERE "T1"."V" = ANY(CAST((SELECT ARRAY_AGG("S"."V") FROM "PUBLIC"."T1" "S" /* PUBLIC.T1_V_IDX */) AS INTEGER ARRAY))

SELECT * FROM T1 WHERE T1.V = ANY(CAST((SELECT ARRAY_AGG(S.V) FROM T1 S) AS INTEGER ARRAY));
> V
> -
> 0
> 1
> 2
> 3
> rows: 4

SELECT V, A, CASE V WHEN = ANY(A) THEN 1 WHEN > ANY(A) THEN 2 WHEN < ANY(A) THEN 3 ELSE 4 END C FROM T1 JOIN T2;
> V    A            C
> ---- ------------ -
> 0    [1, 2, null] 3
> 0    [1, 2]       3
> 0    [1, null]    3
> 0    [1]          3
> 0    []           4
> 0    [null]       4
> 0    null         4
> 1    [1, 2, null] 1
> 1    [1, 2]       1
> 1    [1, null]    1
> 1    [1]          1
> 1    []           4
> 1    [null]       4
> 1    null         4
> 2    [1, 2, null] 1
> 2    [1, 2]       1
> 2    [1, null]    2
> 2    [1]          2
> 2    []           4
> 2    [null]       4
> 2    null         4
> 3    [1, 2, null] 2
> 3    [1, 2]       2
> 3    [1, null]    2
> 3    [1]          2
> 3    []           4
> 3    [null]       4
> 3    null         4
> null [1, 2, null] 4
> null [1, 2]       4
> null [1, null]    4
> null [1]          4
> null []           4
> null [null]       4
> null null         4
> rows: 35
