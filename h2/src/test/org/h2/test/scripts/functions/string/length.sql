-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select length(null) en, len(null) en2, length('This has 17 chars') e_17, len('MSSQLServer uses the len keyword') e_32 from test;
> EN   EN2  E_17 E_32
> ---- ---- ---- ----
> null null 17   32
> rows: 1