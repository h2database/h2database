-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT ID, GREATEST(A, B, C), GREATEST(A, B, C) RESPECT NULLS, GREATEST(A, B, C) IGNORE NULLS FROM (VALUES
    (1, NULL, NULL, NULL),
    (2, NULL, 2, 1),
    (3, 3, 5, NULL),
    (4, 2, 6, 8))
    T(ID, A, B, C);
> ID GREATEST(A, B, C) RESPECT NULLS GREATEST(A, B, C) RESPECT NULLS GREATEST(A, B, C) IGNORE NULLS
> -- ------------------------------- ------------------------------- ------------------------------
> 1  null                            null                            null
> 2  null                            null                            2
> 3  null                            null                            5
> 4  8                               8                               8
> rows: 4

SELECT ID, GREATEST(A, B, C) FROM (VALUES
    (1, (1, 1), (1, 2), (1, 3)),
    (2, (1, NULL), (1, NULL), (1, NULL)),
    (3, (1, NULL), (1, NULL), (2, NULL)),
    (4, (2, NULL), (2, NULL), (1, NULL)),
    (5, (1, NULL), (2, NULL), (1, NULL)),
    (6, (2, NULL), (1, NULL), (2, NULL)),
    (7, (1, 1), (NULL, 1), (NULL, 2)),
    (8, (1, NULL), (NULL, NULL), (2, NULL)))
    T(ID, A, B, C);
> ID GREATEST(A, B, C) RESPECT NULLS
> -- -------------------------------
> 1  ROW (1, 3)
> 2  null
> 3  ROW (2, null)
> 4  null
> 5  ROW (2, null)
> 6  null
> 7  null
> 8  null
> rows: 8
