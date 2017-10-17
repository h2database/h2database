-- Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select degrees(null) vn, degrees(1) v1, degrees(1.1) v2, degrees(-1.1) v3, degrees(1.9) v4, degrees(-1.9) v5 from test;
> VN   V1                V2                V3                 V4                 V5
> ---- ----------------- ----------------- ------------------ ------------------ -------------------
> null 57.29577951308232 63.02535746439057 -63.02535746439057 108.86198107485642 -108.86198107485642
> rows: 1



