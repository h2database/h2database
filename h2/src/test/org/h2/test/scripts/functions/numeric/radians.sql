-- Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select radians(null) vn, radians(1) v1, radians(1.1) v2, radians(-1.1) v3, radians(1.9) v4, radians(-1.9) v5 from test;
> VN   V1                   V2                   V3                    V4                  V5
> ---- -------------------- -------------------- --------------------- ------------------- --------------------
> null 0.017453292519943295 0.019198621771937624 -0.019198621771937624 0.03316125578789226 -0.03316125578789226
> rows: 1



