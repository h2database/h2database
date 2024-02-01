-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select acos(null) vn, acos(-1) r1;
> VN   R1
> ---- -----------------
> null 3.141592653589793
> rows: 1

SELECT ACOS(-1.1);
> exception INVALID_VALUE_2

SELECT ACOS(1.1);
> exception INVALID_VALUE_2

SELECT ACOS(CAST('Infinity' AS DOUBLE PRECISION));
> exception INVALID_VALUE_2
