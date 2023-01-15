-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select nullif(null, null) xn, nullif('a', 'a') xn, nullif('1', '2') x1;
> XN   XN   X1
> ---- ---- --
> null null 1
> rows: 1

SELECT
    A = B,
    NULLIF(A, B), CASE WHEN A = B THEN NULL ELSE A END
    FROM (VALUES
        (1, (1, NULL), (1, NULL)),
        (2, (1, NULL), (2, NULL)),
        (3, (2, NULL), (1, NULL)),
        (4, (1, 1), (1, 2))
    ) T(N, A, B) ORDER BY N;
> A = B NULLIF(A, B)  CASE WHEN A = B THEN NULL ELSE A END
> ----- ------------- ------------------------------------
> null  ROW (1, null) ROW (1, null)
> FALSE ROW (1, null) ROW (1, null)
> FALSE ROW (2, null) ROW (2, null)
> FALSE ROW (1, 1)    ROW (1, 1)
> rows (ordered): 4
