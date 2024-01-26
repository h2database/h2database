-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT (R).A, (R).B FROM (VALUES CAST((1, 2) AS ROW(A INT, B INT))) T(R);
> (R).A (R).B
> ----- -----
> 1     2
> rows: 1

SELECT (R).C FROM (VALUES CAST((1, 2) AS ROW(A INT, B INT))) T(R);
> exception COLUMN_NOT_FOUND_1

SELECT (R).C1, (R).C2 FROM (VALUES ((1, 2))) T(R);
> (R).C1 (R).C2
> ------ ------
> 1      2
> rows: 1

SELECT (1, 2).C2;
>> 2

SELECT (1, 2).C0;
> exception COLUMN_NOT_FOUND_1

SELECT (1, 2).C;
> exception COLUMN_NOT_FOUND_1

SELECT (1, 2).CX;
> exception COLUMN_NOT_FOUND_1

SELECT JSON '{"a": 4, "b": 5, "c": 6}'."b";
>> 5

SELECT JSON '{"a": 4, "b": {"x": 8, "y": 9}, "c": 6}'."b"."y";
>> 9

SELECT JSON '{"a": 4, "b": 5, "c": 6}'."d";
>> null

SELECT JSON '[1]'."d";
>> null
