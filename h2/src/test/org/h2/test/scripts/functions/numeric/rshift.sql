-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select rshift(null, 1) vn, rshift(1, null) vn1, rshift(null, null) vn2, rshift(3, 6) v1, rshift(1024,3) v2;
> VN   VN1  VN2  V1 V2
> ---- ---- ---- -- ---
> null null null 0  128
> rows: 1

SELECT I,
    RSHIFT(CAST(-128 AS TINYINT), I), RSHIFT(CAST(1 AS TINYINT), I),
    URSHIFT(CAST(-128 AS TINYINT), I), URSHIFT(CAST(1 AS TINYINT), I)
    FROM
    (VALUES -111, -8, -7, -1, 0, 1, 7, 8, 111) T(I) ORDER BY I;
> I    RSHIFT(-128, I) RSHIFT(1, I) URSHIFT(-128, I) URSHIFT(1, I)
> ---- --------------- ------------ ---------------- -------------
> -111 0               0            0                0
> -8   0               0            0                0
> -7   0               -128         0                -128
> -1   0               2            0                2
> 0    -128            1            -128             1
> 1    -64             0            64               0
> 7    -1              0            1                0
> 8    -1              0            0                0
> 111  -1              0            0                0
> rows (ordered): 9

SELECT I,
    RSHIFT(CAST(-32768 AS SMALLINT), I), RSHIFT(CAST(1 AS SMALLINT), I),
    URSHIFT(CAST(-32768 AS SMALLINT), I), URSHIFT(CAST(1 AS SMALLINT), I)
    FROM
    (VALUES -111, -16, -15, -1, 0, 1, 15, 16, 111) T(I) ORDER BY I;
> I    RSHIFT(-32768, I) RSHIFT(1, I) URSHIFT(-32768, I) URSHIFT(1, I)
> ---- ----------------- ------------ ------------------ -------------
> -111 0                 0            0                  0
> -16  0                 0            0                  0
> -15  0                 -32768       0                  -32768
> -1   0                 2            0                  2
> 0    -32768            1            -32768             1
> 1    -16384            0            16384              0
> 15   -1                0            1                  0
> 16   -1                0            0                  0
> 111  -1                0            0                  0
> rows (ordered): 9

SELECT I,
    RSHIFT(CAST(-2147483648 AS INTEGER), I), RSHIFT(CAST(1 AS INTEGER), I),
    URSHIFT(CAST(-2147483648 AS INTEGER), I), URSHIFT(CAST(1 AS INTEGER), I)
    FROM
    (VALUES -111, -32, -31, -1, 0, 1, 31, 32, 111) T(I) ORDER BY I;
> I    RSHIFT(-2147483648, I) RSHIFT(1, I) URSHIFT(-2147483648, I) URSHIFT(1, I)
> ---- ---------------------- ------------ ----------------------- -------------
> -111 0                      0            0                       0
> -32  0                      0            0                       0
> -31  0                      -2147483648  0                       -2147483648
> -1   0                      2            0                       2
> 0    -2147483648            1            -2147483648             1
> 1    -1073741824            0            1073741824              0
> 31   -1                     0            1                       0
> 32   -1                     0            0                       0
> 111  -1                     0            0                       0
> rows (ordered): 9

SELECT I,
    RSHIFT(CAST(-9223372036854775808 AS BIGINT), I), RSHIFT(CAST(1 AS BIGINT), I),
    URSHIFT(CAST(-9223372036854775808 AS BIGINT), I), URSHIFT(CAST(1 AS BIGINT), I)
    FROM
    (VALUES -111, -64, -63, -1, 0, 1, 63, 64, 111) T(I) ORDER BY I;
> I    RSHIFT(-9223372036854775808, I) RSHIFT(1, I)         URSHIFT(-9223372036854775808, I) URSHIFT(1, I)
> ---- ------------------------------- -------------------- -------------------------------- --------------------
> -111 0                               0                    0                                0
> -64  0                               0                    0                                0
> -63  0                               -9223372036854775808 0                                -9223372036854775808
> -1   0                               2                    0                                2
> 0    -9223372036854775808            1                    -9223372036854775808             1
> 1    -4611686018427387904            0                    4611686018427387904              0
> 63   -1                              0                    1                                0
> 64   -1                              0                    0                                0
> 111  -1                              0                    0                                0
> rows (ordered): 9

SELECT RSHIFT(X'', 1);
>> X''

SELECT RSHIFT(CAST(X'02' AS BINARY), 1);
>> X'01'

SELECT I, RSHIFT(X'80ABCD09', I) FROM
    (VALUES -33, -32, -31, -17, -16, -15, -1, 0, 1, 15, 16, 17, 31, 32, 33) T(I) ORDER BY I;
> I   RSHIFT(X'80abcd09', I)
> --- ----------------------
> -33 X'00000000'
> -32 X'00000000'
> -31 X'80000000'
> -17 X'9a120000'
> -16 X'cd090000'
> -15 X'e6848000'
> -1  X'01579a12'
> 0   X'80abcd09'
> 1   X'4055e684'
> 15  X'00010157'
> 16  X'000080ab'
> 17  X'00004055'
> 31  X'00000001'
> 32  X'00000000'
> 33  X'00000000'
> rows (ordered): 15

SELECT RSHIFT(-1, -9223372036854775808);
>> 0

SELECT URSHIFT(-1, -9223372036854775808);
>> 0
