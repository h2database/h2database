-- Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT ARRAY_AGG(V) L, GCD_AGG(V), LCM_AGG(V)
    FROM (VALUES (1, NULL), (1, 1), (2, 1), (2, NULL), (3, NULL), (3, NULL),
    (4, 1), (4, 6), (5, 6), (5, -1), (6, 6), (6, 8), (7, -6), (7, 8), (8, 6), (8, -8), (9, -6), (9, -8),
    (10, 0), (10, 2), (11, 2), (11, 0), (12, 0), (12, 0)) T(G, V)
    GROUP BY G ORDER BY G;
> L            GCD_AGG(V) LCM_AGG(V)
> ------------ ---------- ----------
> [null, 1]    1          1
> [1, null]    1          1
> [null, null] null       null
> [1, 6]       1          6
> [6, -1]      1          6
> [6, 8]       2          24
> [-6, 8]      2          24
> [6, -8]      2          24
> [-6, -8]     2          24
> [0, 2]       2          0
> [2, 0]       2          0
> [0, 0]       0          0
> rows (ordered): 12

SELECT LCM_AGG(V) FROM (VALUES CAST(1E99999 AS NUMERIC), CAST(1.1E99999 AS NUMERIC)) T(V);
> exception VALUE_TOO_LONG_2

SELECT LCM_AGG(V) FROM (VALUES CAST(1E99999 AS NUMERIC), CAST(1.1E99999 AS NUMERIC), CAST(9.2E99999 AS NUMERIC)) T(V);
> exception VALUE_TOO_LONG_2

SELECT LCM_AGG(V) FROM (VALUES CAST(1E49999 AS NUMERIC), CAST(1.1E49999 AS NUMERIC),
    CAST(9.0000001E99999 AS NUMERIC)) T(V);
> exception VALUE_TOO_LONG_2

SELECT LCM_AGG(V) FROM (VALUES CAST(1E49999 AS NUMERIC), CAST(1.1E49999 AS NUMERIC),
    CAST(9.0000001E99999 AS NUMERIC), CAST(9.0000002E99999 AS NUMERIC)) T(V);
> exception VALUE_TOO_LONG_2

SELECT LCM_AGG(V) FROM (VALUES CAST(1E99999 AS NUMERIC), CAST(1.1E99999 AS NUMERIC), 0) T(V);
>> 0
