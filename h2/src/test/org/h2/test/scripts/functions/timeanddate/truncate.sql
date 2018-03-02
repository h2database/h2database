-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select trunc('2015-05-29 15:00:00') R;
> R
> ---------------------
> 2015-05-29 00:00:00.0
> rows: 1

select trunc('2015-05-29') R;
> R
> ---------------------
> 2015-05-29 00:00:00.0
> rows: 1

select trunc(timestamp '2000-01-01 10:20:30.0') R;
> R
> ---------------------
> 2000-01-01 00:00:00.0
> rows: 1

select trunc(timestamp '2001-01-01 14:00:00.0') R;
> R
> ---------------------
> 2001-01-01 00:00:00.0
> rows: 1
