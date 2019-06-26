-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select lshift(null, 1) vn, lshift(1, null) vn1, lshift(null, null) vn2, lshift(3, 6) v1, lshift(3,0) v2;
> VN   VN1  VN2  V1  V2
> ---- ---- ---- --- --
> null null null 192 3
> rows: 1
