-- Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT A, B, GCD(A, B), LCM(A, B)
    FROM (VALUES (NULL, 1), (1, NULL), (NULL, NULL),
    (1, 6), (6, -1), (6, 8), (-6, 8), (6, -8), (-6, -8),
    (0, 2), (2, 0), (0, 0)) T(A, B);
> A    B    GCD(A, B) LCM(A, B)
> ---- ---- --------- ---------
> -6   -8   2         24
> -6   8    2         24
> 0    0    0         0
> 0    2    2         0
> 1    6    1         6
> 1    null null      null
> 2    0    2         0
> 6    -1   1         6
> 6    -8   2         24
> 6    8    2         24
> null 1    null      null
> null null null      null
> rows: 12

SELECT GCD(32, 12, 0, 40);
>> 4

SELECT GCD(32, 9, 40);
>> 1

SELECT GCD(32, 12, NULL);
>> null

SELECT GCD(32, 12, CAST(NULL AS INTEGER));
>> null

SELECT LCM(6, 9, 5, 0, 3);
>> 0

SELECT LCM(6, 9, 5, 22);
>> 990

SELECT LCM(CAST(1E99999 AS NUMERIC), CAST(1.1E99999 AS NUMERIC));
> exception VALUE_TOO_LONG_2

SELECT LCM(CAST(9E99999 AS NUMERIC), CAST(9.1E99999 AS NUMERIC), CAST(9.2E99999 AS NUMERIC));
> exception VALUE_TOO_LONG_2

SELECT LCM(CAST(1E49999 AS NUMERIC), CAST(1.1E49999 AS NUMERIC), CAST(9.0000001E99999 AS NUMERIC));
> exception VALUE_TOO_LONG_2

SELECT LCM(CAST(1E99999 AS NUMERIC), 0, CAST(1.1E99999 AS NUMERIC));
>> 0

SELECT LCM(CAST(1E99999 AS NUMERIC), CAST(1.1E99999 AS NUMERIC), 0);
>> 0

SELECT LCM(CAST(1E99999 AS NUMERIC), CAST(1.1E99999 AS NUMERIC), NULL);
>> null

SELECT LCM(CAST(1E99999 AS NUMERIC), CAST(1.1E99999 AS NUMERIC), CAST(NULL AS NUMERIC));
>> null

SELECT GCD(A, B), LCM(A, B) FROM (VALUES (CAST(6 AS TINYINT), CAST(10 AS TINYINT))) T(A, B);
> GCD(A, B) LCM(A, B)
> --------- ---------
> 2         30
> rows: 1

SELECT GCD(A, B), LCM(A, B) FROM (VALUES (CAST(6 AS SMALLINT), CAST(10 AS SMALLINT))) T(A, B);
> GCD(A, B) LCM(A, B)
> --------- ---------
> 2         30
> rows: 1

SELECT GCD(A, B), LCM(A, B) FROM (VALUES (CAST(6 AS INTEGER), CAST(10 AS INTEGER))) T(A, B);
> GCD(A, B) LCM(A, B)
> --------- ---------
> 2         30
> rows: 1

SELECT GCD(A, B), LCM(A, B) FROM (VALUES (CAST(6 AS BIGINT), CAST(10 AS BIGINT))) T(A, B);
> GCD(A, B) LCM(A, B)
> --------- ---------
> 2         30
> rows: 1

SELECT GCD(A, B), LCM(A, B) FROM (VALUES (CAST(6 AS NUMERIC), CAST(10 AS NUMERIC))) T(A, B);
> GCD(A, B) LCM(A, B)
> --------- ---------
> 2         30
> rows: 1

SELECT GCD(A, B), LCM(A, B) FROM (VALUES (CAST(6 AS NUMERIC(10, 2)), CAST(10 AS NUMERIC(10, 2)))) T(A, B);
> exception INVALID_VALUE_2

SELECT GCD(A, B), LCM(A, B) FROM (VALUES (CAST(6 AS REAL), CAST(10 AS REAL))) T(A, B);
> exception INVALID_VALUE_2

EXPLAIN SELECT GCD(A, GCD(B, C, D), E, GCD(F, G, H), I)
    FROM (VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9)) T(A, B, C, D, E, F, G, H, I);
>> SELECT GCD("A", "B", "C", "D", "E", "F", "G", "H", "I") FROM (VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9)) "T"("A", "B", "C", "D", "E", "F", "G", "H", "I") /* table scan */
