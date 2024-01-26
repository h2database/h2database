-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select lshift(null, 1) vn, lshift(1, null) vn1, lshift(null, null) vn2, lshift(3, 6) v1, lshift(3,0) v2;
> VN   VN1  VN2  V1  V2
> ---- ---- ---- --- --
> null null null 192 3
> rows: 1

SELECT I,
    LSHIFT(CAST(-128 AS TINYINT), I), LSHIFT(CAST(1 AS TINYINT), I),
    ULSHIFT(CAST(-128 AS TINYINT), I), ULSHIFT(CAST(1 AS TINYINT), I)
    FROM
    (VALUES -111, -8, -7, -1, 0, 1, 7, 8, 111) T(I) ORDER BY I;
> I    LSHIFT(-128, I) LSHIFT(1, I) ULSHIFT(-128, I) ULSHIFT(1, I)
> ---- --------------- ------------ ---------------- -------------
> -111 -1              0            0                0
> -8   -1              0            0                0
> -7   -1              0            1                0
> -1   -64             0            64               0
> 0    -128            1            -128             1
> 1    0               2            0                2
> 7    0               -128         0                -128
> 8    0               0            0                0
> 111  0               0            0                0
> rows (ordered): 9

SELECT I,
    LSHIFT(CAST(-32768 AS SMALLINT), I), LSHIFT(CAST(1 AS SMALLINT), I),
    ULSHIFT(CAST(-32768 AS SMALLINT), I), ULSHIFT(CAST(1 AS SMALLINT), I)
    FROM
    (VALUES -111, -16, -15, -1, 0, 1, 15, 16, 111) T(I) ORDER BY I;
> I    LSHIFT(-32768, I) LSHIFT(1, I) ULSHIFT(-32768, I) ULSHIFT(1, I)
> ---- ----------------- ------------ ------------------ -------------
> -111 -1                0            0                  0
> -16  -1                0            0                  0
> -15  -1                0            1                  0
> -1   -16384            0            16384              0
> 0    -32768            1            -32768             1
> 1    0                 2            0                  2
> 15   0                 -32768       0                  -32768
> 16   0                 0            0                  0
> 111  0                 0            0                  0
> rows (ordered): 9

SELECT I,
    LSHIFT(CAST(-2147483648 AS INTEGER), I), LSHIFT(CAST(1 AS INTEGER), I),
    ULSHIFT(CAST(-2147483648 AS INTEGER), I), ULSHIFT(CAST(1 AS INTEGER), I)
    FROM
    (VALUES -111, -32, -31, -1, 0, 1, 31, 32, 111) T(I) ORDER BY I;
> I    LSHIFT(-2147483648, I) LSHIFT(1, I) ULSHIFT(-2147483648, I) ULSHIFT(1, I)
> ---- ---------------------- ------------ ----------------------- -------------
> -111 -1                     0            0                       0
> -32  -1                     0            0                       0
> -31  -1                     0            1                       0
> -1   -1073741824            0            1073741824              0
> 0    -2147483648            1            -2147483648             1
> 1    0                      2            0                       2
> 31   0                      -2147483648  0                       -2147483648
> 32   0                      0            0                       0
> 111  0                      0            0                       0
> rows (ordered): 9

SELECT I,
    LSHIFT(CAST(-9223372036854775808 AS BIGINT), I), LSHIFT(CAST(1 AS BIGINT), I),
    ULSHIFT(CAST(-9223372036854775808 AS BIGINT), I), ULSHIFT(CAST(1 AS BIGINT), I)
    FROM
    (VALUES -111, -64, -63, -1, 0, 1, 63, 64, 111) T(I) ORDER BY I;
> I    LSHIFT(-9223372036854775808, I) LSHIFT(1, I)         ULSHIFT(-9223372036854775808, I) ULSHIFT(1, I)
> ---- ------------------------------- -------------------- -------------------------------- --------------------
> -111 -1                              0                    0                                0
> -64  -1                              0                    0                                0
> -63  -1                              0                    1                                0
> -1   -4611686018427387904            0                    4611686018427387904              0
> 0    -9223372036854775808            1                    -9223372036854775808             1
> 1    0                               2                    0                                2
> 63   0                               -9223372036854775808 0                                -9223372036854775808
> 64   0                               0                    0                                0
> 111  0                               0                    0                                0
> rows (ordered): 9

SELECT LSHIFT(X'', 1);
>> X''

SELECT LSHIFT(CAST(X'02' AS BINARY), 1);
>> X'04'

SELECT I, LSHIFT(X'80ABCD09', I) FROM
    (VALUES -33, -32, -31, -17, -16, -15, -1, 0, 1, 15, 16, 17, 31, 32, 33) T(I) ORDER BY I;
> I   LSHIFT(X'80abcd09', I)
> --- ----------------------
> -33 X'00000000'
> -32 X'00000000'
> -31 X'00000001'
> -17 X'00004055'
> -16 X'000080ab'
> -15 X'00010157'
> -1  X'4055e684'
> 0   X'80abcd09'
> 1   X'01579a12'
> 15  X'e6848000'
> 16  X'cd090000'
> 17  X'9a120000'
> 31  X'80000000'
> 32  X'00000000'
> 33  X'00000000'
> rows (ordered): 15
