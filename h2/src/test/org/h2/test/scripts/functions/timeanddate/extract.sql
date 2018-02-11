-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select EXTRACT (EPOCH from time '00:00:00');
> 0

select EXTRACT (EPOCH from time '10:00:00');
> 36000

select EXTRACT (EPOCH from time '10:00:00.123456');
> 36000.123456

select EXTRACT (EPOCH from date '1970-01-01');
> 0

select EXTRACT (EPOCH from date '2000-01-03');
> 946857600

select EXTRACT (EPOCH from timestamp '1970-01-01 00:00:00');
> 0

select EXTRACT (EPOCH from timestamp '1970-01-03 12:00:00.123456');
> 216000.123456

select EXTRACT (EPOCH from timestamp '2000-01-03 12:00:00.123456');
> 946900800.123456

select EXTRACT (EPOCH from timestamp '2500-01-03 12:00:00.654321');
> 16725441600.654321

select EXTRACT (EPOCH from timestamp with time zone '1970-01-01 00:00:00+05');
> -18000

select EXTRACT (EPOCH from timestamp with time zone '1970-01-03 12:00:00.123456+05');
> 198000.123456

select EXTRACT (EPOCH from timestamp with time zone '2000-01-03 12:00:00.123456+05');
> 946882800.123456

select extract(EPOCH from '2001-02-03 14:15:16');
> 981209716
