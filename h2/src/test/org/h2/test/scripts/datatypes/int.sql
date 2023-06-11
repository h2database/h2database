-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- Division

SELECT CAST(1 AS INT) / CAST(0 AS INT);
> exception DIVISION_BY_ZERO_1

SELECT CAST(-2147483648 AS INT) / CAST(1 AS INT);
>> -2147483648

SELECT CAST(-2147483648 AS INT) / CAST(-1 AS INT);
> exception NUMERIC_VALUE_OUT_OF_RANGE_1

EXPLAIN VALUES 1;
>> VALUES (1)

SELECT 0x100, 0o100, 0b100;
> 256 64 4
> --- -- -
> 256 64 4
> rows: 1

SELECT 100_000, 1_1_1, 0b_1_1, 0o_1_1, 0x_1_1;
> 100000 111 3 9 17
> ------ --- - - --
> 100000 111 3 9 17
> rows: 1

SELECT 1_;
> exception SYNTAX_ERROR_2

SELECT _1;
> exception COLUMN_NOT_FOUND_1

SELECT 1__1;
> exception SYNTAX_ERROR_2

SELECT 0x__1;
> exception SYNTAX_ERROR_2

SELECT 0x1_;
> exception SYNTAX_ERROR_2
