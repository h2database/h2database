-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select locate(null, null) en, locate(null, null, null) en1;
> EN   EN1
> ---- ----
> null null
> rows: 1

select locate('World', 'Hello World') e7, locate('hi', 'abchihihi', 2) e3;
> E7 E3
> -- --
> 7  4
> rows: 1

SELECT CHARINDEX('test', 'test');
> exception FUNCTION_NOT_FOUND_1

SET MODE MSSQLServer;
> ok

select charindex('World', 'Hello World') e7, charindex('hi', 'abchihihi', 2) e3;
> E7 E3
> -- --
> 7  4
> rows: 1

SET MODE Regular;
> ok

select instr('Hello World', 'World') e7, instr('abchihihi', 'hi', 2) e3, instr('abcooo', 'o') e2;
> E7 E3 E2
> -- -- --
> 7  4  4
> rows: 1

EXPLAIN SELECT INSTR(A, B) FROM (VALUES ('A', 'B')) T(A, B);
>> SELECT LOCATE("B", "A") FROM (VALUES ('A', 'B')) "T"("A", "B") /* table scan */

select position(null, null) en, position(null, 'abc') en1, position('World', 'Hello World') e7, position('hi', 'abchihihi') e1;
> EN   EN1  E7 E1
> ---- ---- -- --
> null null 7  4
> rows: 1

EXPLAIN SELECT POSITION((A > B), C) FROM (VALUES (1, 2, 3)) T(A, B, C);
>> SELECT LOCATE("A" > "B", "C") FROM (VALUES (1, 2, 3)) "T"("A", "B", "C") /* table scan */
