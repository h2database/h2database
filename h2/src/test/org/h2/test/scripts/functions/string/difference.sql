-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select difference(null, null) en, difference('a', null) en1, difference(null, 'a') en2;
> EN   EN1  EN2
> ---- ---- ----
> null null null
> rows: 1

select difference('abc', 'abc') e0, difference('Thomas', 'Tom') e1;
> E0 E1
> -- --
> 4  3
> rows: 1
