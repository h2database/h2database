-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT I,
    BITGET(CAST((0xC5 - 0x100) AS TINYINT), I),
    BITGET(CAST(0xC5 AS SMALLINT), I),
    BITGET(CAST(0xC5 AS INTEGER), I),
    BITGET(CAST(0xC5 AS BIGINT), I),
    BITGET(CAST(X'C5' AS VARBINARY), I),
    BITGET(CAST(X'C5' AS BINARY), I)
    FROM (VALUES -1, 0, 1, 4, 9, 99) T(I);
> I  BITGET(-59, I) BITGET(197, I) BITGET(197, I) BITGET(197, I) BITGET(CAST(X'c5' AS BINARY VARYING), I) BITGET(X'c5', I)
> -- -------------- -------------- -------------- -------------- ---------------------------------------- ----------------
> -1 FALSE          FALSE          FALSE          FALSE          FALSE                                    FALSE
> 0  TRUE           TRUE           TRUE           TRUE           TRUE                                     TRUE
> 1  FALSE          FALSE          FALSE          FALSE          FALSE                                    FALSE
> 4  FALSE          FALSE          FALSE          FALSE          FALSE                                    FALSE
> 9  FALSE          FALSE          FALSE          FALSE          FALSE                                    FALSE
> 99 FALSE          FALSE          FALSE          FALSE          FALSE                                    FALSE
> rows: 6

SELECT X, BITGET(X'1001', X) FROM SYSTEM_RANGE(7, 9);
> X BITGET(X'1001', X)
> - ------------------
> 7 FALSE
> 8 TRUE
> 9 FALSE
> rows: 3
