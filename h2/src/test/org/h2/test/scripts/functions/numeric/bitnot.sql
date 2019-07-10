-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: Joe Littlejohn
--

select bitnot(null) vn, bitnot(0) v1, bitnot(10) v2, bitnot(-10) v3;
> VN   V1 V2  V3
> ---- -- --- --
> null -1 -11 9
> rows: 1
