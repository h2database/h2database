-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select dateadd('month', 1, timestamp '2003-01-31 10:20:30.012345678') d1 from test;
> D1
> -----------------------------
> 2003-02-28 10:20:30.012345678
> rows: 1

select dateadd('year', -1, timestamp '2000-02-29 10:20:30.012345678') d1 from test;
> D1
> -----------------------------
> 1999-02-28 10:20:30.012345678
> rows: 1

drop table test;
> ok

create table test(d date, t time, ts timestamp);
> ok

insert into test values(date '2001-01-01', time '01:00:00', timestamp '2010-01-01 00:00:00');
> update count: 1

select ts + t x from test;
> X
> ---------------------
> 2010-01-01 01:00:00.0
> rows: 1

select ts + t + t - t x from test;
> X
> ---------------------
> 2010-01-01 01:00:00.0
> rows: 1

select ts + t * 0.5 x from test;
> X
> ---------------------
> 2010-01-01 00:30:00.0
> rows: 1

select ts + 0.5 x from test;
> X
> ---------------------
> 2010-01-01 12:00:00.0
> rows: 1

select ts - 1.5 x from test;
> X
> ---------------------
> 2009-12-30 12:00:00.0
> rows: 1

select ts + 0.5 * t + t - t x from test;
> X
> ---------------------
> 2010-01-01 00:30:00.0
> rows: 1

select ts + t / 0.5 x from test;
> X
> ---------------------
> 2010-01-01 02:00:00.0
> rows: 1

select d + t, t + d - t x from test;
> T + D                 X
> --------------------- ---------------------
> 2001-01-01 01:00:00.0 2001-01-01 00:00:00.0
> rows: 1

select 1 + d + 1, d - 1, 2 + ts + 2, ts - 2 from test;
> DATEADD('DAY', 1, DATEADD('DAY', 1, D)) DATEADD('DAY', -1, D) DATEADD('DAY', 2, DATEADD('DAY', 2, TS)) DATEADD('DAY', -2, TS)
> --------------------------------------- --------------------- ---------------------------------------- ----------------------
> 2001-01-03 00:00:00.0                   2000-12-31 00:00:00.0 2010-01-05 00:00:00.0                    2009-12-30 00:00:00.0
> rows: 1

select 1 + d + t + 1 from test;
> DATEADD('DAY', 1, (T + DATEADD('DAY', 1, D)))
> ---------------------------------------------
> 2001-01-03 01:00:00.0
> rows: 1

select ts - t - 2 from test;
> DATEADD('DAY', -2, (TS - T))
> ----------------------------
> 2009-12-29 23:00:00.0
> rows: 1

drop table test;
> ok

call dateadd('MS', 1, TIMESTAMP '2001-02-03 04:05:06.789001');
> TIMESTAMP '2001-02-03 04:05:06.790001'
> --------------------------------------
> 2001-02-03 04:05:06.790001
> rows: 1
