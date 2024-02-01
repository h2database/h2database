-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT REGR_SYY(Y, X) OVER (ORDER BY R) FROM (VALUES
    (1, NULL, 1),
    (2, 1, NULL),
    (3, NULL, NULL),
    (4, -3, -2),
    (5, -3, -1),
    (6, 10, 9),
    (7, 10, 10),
    (8, 10, 11),
    (9, 11, 7)
) T(R, Y, X) ORDER BY R;
> REGR_SYY(Y, X) OVER (ORDER BY R)
> --------------------------------
> null
> null
> null
> 0.0
> 0.0
> 112.66666666666669
> 169.00000000000003
> 202.80000000000004
> 234.83333333333337
> rows (ordered): 9
