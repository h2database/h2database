-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select datediff('yy', timestamp '2003-12-01 10:20:30.0', timestamp '2004-01-01 10:00:00.0') d1 from test;
> D1
> --
> 1
> rows: 1

select datediff('year', timestamp '2003-12-01 10:20:30.0', timestamp '2004-01-01 10:00:00.0') d1 from test;
> D1
> --
> 1
> rows: 1

select datediff('mm', timestamp '2003-11-01 10:20:30.0', timestamp '2004-01-01 10:00:00.0') d2 from test;
> D2
> --
> 2
> rows: 1

select datediff('month', timestamp '2003-11-01 10:20:30.0', timestamp '2004-01-01 10:00:00.0') d2 from test;
> D2
> --
> 2
> rows: 1

select datediff('dd', timestamp '2004-01-01 10:20:30.0', timestamp '2004-01-05 10:00:00.0') d4 from test;
> D4
> --
> 4
> rows: 1

select datediff('day', timestamp '2004-01-01 10:20:30.0', timestamp '2004-01-05 10:00:00.0') d4 from test;
> D4
> --
> 4
> rows: 1

select datediff('hh', timestamp '2004-01-01 10:20:30.0', timestamp '2004-01-02 10:00:00.0') d24 from test;
> D24
> ---
> 24
> rows: 1

select datediff('hour', timestamp '2004-01-01 10:20:30.0', timestamp '2004-01-02 10:00:00.0') d24 from test;
> D24
> ---
> 24
> rows: 1

select datediff('mi', timestamp '2004-01-01 10:20:30.0', timestamp '2004-01-01 10:00:00.0') d20 from test;
> D20
> ---
> -20
> rows: 1

select datediff('minute', timestamp '2004-01-01 10:20:30.0', timestamp '2004-01-01 10:00:00.0') d20 from test;
> D20
> ---
> -20
> rows: 1

select datediff('ss', timestamp '2004-01-01 10:00:00.5', timestamp '2004-01-01 10:00:01.0') d1 from test;
> D1
> --
> 1
> rows: 1

select datediff('second', timestamp '2004-01-01 10:00:00.5', timestamp '2004-01-01 10:00:01.0') d1 from test;
> D1
> --
> 1
> rows: 1

select datediff('ms', timestamp '2004-01-01 10:00:00.5', timestamp '2004-01-01 10:00:01.0') d50x from test;
> D50X
> ----
> 500
> rows: 1

select datediff('millisecond', timestamp '2004-01-01 10:00:00.5', timestamp '2004-01-01 10:00:01.0') d50x from test;
> D50X
> ----
> 500
> rows: 1

SELECT DATEDIFF('SECOND', '1900-01-01 00:00:00.001', '1900-01-01 00:00:00.002'), DATEDIFF('SECOND', '2000-01-01 00:00:00.001', '2000-01-01 00:00:00.002');
> 0 0
> - -
> 0 0
> rows: 1

SELECT DATEDIFF('SECOND', '1900-01-01 00:00:00.000', '1900-01-01 00:00:00.001'), DATEDIFF('SECOND', '2000-01-01 00:00:00.000', '2000-01-01 00:00:00.001');
> 0 0
> - -
> 0 0
> rows: 1

SELECT DATEDIFF('MINUTE', '1900-01-01 00:00:00.000', '1900-01-01 00:00:01.000'), DATEDIFF('MINUTE', '2000-01-01 00:00:00.000', '2000-01-01 00:00:01.000');
> 0 0
> - -
> 0 0
> rows: 1

SELECT DATEDIFF('MINUTE', '1900-01-01 00:00:01.000', '1900-01-01 00:00:02.000'), DATEDIFF('MINUTE', '2000-01-01 00:00:01.000', '2000-01-01 00:00:02.000');
> 0 0
> - -
> 0 0
> rows: 1

SELECT DATEDIFF('HOUR', '1900-01-01 00:00:00.000', '1900-01-01 00:00:01.000'), DATEDIFF('HOUR', '2000-01-01 00:00:00.000', '2000-01-01 00:00:01.000');
> 0 0
> - -
> 0 0
> rows: 1

SELECT DATEDIFF('HOUR', '1900-01-01 00:00:00.001', '1900-01-01 00:00:01.000'), DATEDIFF('HOUR', '2000-01-01 00:00:00.001', '2000-01-01 00:00:01.000');
> 0 0
> - -
> 0 0
> rows: 1

SELECT DATEDIFF('HOUR', '1900-01-01 01:00:00.000', '1900-01-01 01:00:01.000'), DATEDIFF('HOUR', '2000-01-01 01:00:00.000', '2000-01-01 01:00:01.000');
> 0 0
> - -
> 0 0
> rows: 1

SELECT DATEDIFF('HOUR', '1900-01-01 01:00:00.001', '1900-01-01 01:00:01.000'), DATEDIFF('HOUR', '2000-01-01 01:00:00.001', '2000-01-01 01:00:01.000');
> 0 0
> - -
> 0 0
> rows: 1

select datediff(day, '2015-12-09 23:59:00.0', '2016-01-16 23:59:00.0'), datediff(wk, '2015-12-09 23:59:00.0', '2016-01-16 23:59:00.0');
> 38 5
> -- -
> 38 5
> rows: 1

call datediff('MS', TIMESTAMP '2001-02-03 04:05:06.789001', TIMESTAMP '2001-02-03 04:05:06.789002');
> 0
> -
> 0
> rows: 1

call datediff('MS', TIMESTAMP '1900-01-01 00:00:01.000', TIMESTAMP '2008-01-01 00:00:00.000');
> 3408134399000
> -------------
> 3408134399000
> rows: 1
