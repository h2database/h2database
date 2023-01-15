-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT I, ROTATELEFT(CAST(0x7d AS TINYINT), I) L, ROTATERIGHT(CAST(0x7d AS TINYINT), I) R
    FROM (VALUES -8, -7, -2, -1, 0, 1, 2, 7, 8) T(I) ORDER BY I;
> I  L   R
> -- --- ---
> -8 125 125
> -7 -6  -66
> -2 95  -11
> -1 -66 -6
> 0  125 125
> 1  -6  -66
> 2  -11 95
> 7  -66 -6
> 8  125 125
> rows (ordered): 9

SELECT I, ROTATELEFT(CAST(0x6d3f AS SMALLINT), I) L, ROTATERIGHT(CAST(0x6d3f AS SMALLINT), I) R
    FROM (VALUES -16, -15, -2, -1, 0, 1, 2, 15, 16) T(I) ORDER BY I;
> I   L      R
> --- ------ ------
> -16 27967  27967
> -15 -9602  -18785
> -2  -9393  -19203
> -1  -18785 -9602
> 0   27967  27967
> 1   -9602  -18785
> 2   -19203 -9393
> 15  -18785 -9602
> 16  27967  27967
> rows (ordered): 9

SELECT I, ROTATELEFT(CAST(0x7d12e43c AS INTEGER), I) L, ROTATERIGHT(CAST(0x7d12e43c AS INTEGER), I) R
    FROM (VALUES -32, -31, -2, -1, 0, 1, 2, 31, 32) T(I) ORDER BY I;
> I   L          R
> --- ---------- ----------
> -32 2098390076 2098390076
> -31 -98187144  1049195038
> -2  524597519  -196374287
> -1  1049195038 -98187144
> 0   2098390076 2098390076
> 1   -98187144  1049195038
> 2   -196374287 524597519
> 31  1049195038 -98187144
> 32  2098390076 2098390076
> rows (ordered): 9

SELECT I, ROTATELEFT(CAST(0x7302abe53d12e45f AS BIGINT), I) L, ROTATERIGHT(CAST(0x7302abe53d12e45f AS BIGINT), I) R
    FROM (VALUES -64, -63, -2, -1, 0, 1, 2, 63, 64) T(I) ORDER BY I;
> I   L                    R
> --- -------------------- --------------------
> -64 8287375265375642719  8287375265375642719
> -63 -1871993542958266178 -5079684404166954449
> -2  -2539842202083477225 -3743987085916532355
> -1  -5079684404166954449 -1871993542958266178
> 0   8287375265375642719  8287375265375642719
> 1   -1871993542958266178 -5079684404166954449
> 2   -3743987085916532355 -2539842202083477225
> 63  -5079684404166954449 -1871993542958266178
> 64  8287375265375642719  8287375265375642719
> rows (ordered): 9

SELECT I, ROTATELEFT(X'ABCD', I) L, ROTATERIGHT(X'ABCD', I) R
    FROM (VALUES -16, -15, -8, -1, 0, 1, 8, 15, 16) T(I) ORDER BY I;
> I   L       R
> --- ------- -------
> -16 X'abcd' X'abcd'
> -15 X'579b' X'd5e6'
> -8  X'cdab' X'cdab'
> -1  X'd5e6' X'579b'
> 0   X'abcd' X'abcd'
> 1   X'579b' X'd5e6'
> 8   X'cdab' X'cdab'
> 15  X'd5e6' X'579b'
> 16  X'abcd' X'abcd'
> rows (ordered): 9

SELECT I, ROTATELEFT(CAST(X'ABCD' AS BINARY(2)), I) L, ROTATERIGHT(CAST(X'ABCD' AS BINARY(2)), I) R
    FROM (VALUES -16, -15, -8, -1, 0, 1, 8, 15, 16) T(I) ORDER BY I;
> I   L       R
> --- ------- -------
> -16 X'abcd' X'abcd'
> -15 X'579b' X'd5e6'
> -8  X'cdab' X'cdab'
> -1  X'd5e6' X'579b'
> 0   X'abcd' X'abcd'
> 1   X'579b' X'd5e6'
> 8   X'cdab' X'cdab'
> 15  X'd5e6' X'579b'
> 16  X'abcd' X'abcd'
> rows (ordered): 9

SELECT ROTATELEFT(X'8000', 1);
>> X'0001'

SELECT ROTATERIGHT(X'0001', 1);
>> X'8000'

SELECT ROTATELEFT(X'', 1);
>> X''
