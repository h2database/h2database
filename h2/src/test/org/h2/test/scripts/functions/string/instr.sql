-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select instr('Hello World', 'World') e7, instr('abchihihi', 'hi', 2) e3, instr('abcooo', 'o') e2;
> E7 E3 E2
> -- -- --
> 7  4  4
> rows: 1
