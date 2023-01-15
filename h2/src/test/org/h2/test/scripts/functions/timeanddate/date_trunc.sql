-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

@reconnect off

SET TIME ZONE '01:00';
> ok

--
-- Test time unit in 'MICROSECONDS'
--
SELECT DATE_TRUNC('MICROSECONDS', time '00:00:00.000');
>> 00:00:00

SELECT DATE_TRUNC('microseconds', time '00:00:00.000');
>> 00:00:00

SELECT DATE_TRUNC(microseconds, time '00:00:00.000');
>> 00:00:00

SELECT DATE_TRUNC('MICROSECONDS', time '15:00:00');
>> 15:00:00

SELECT DATE_TRUNC('microseconds', time '15:00:00');
>> 15:00:00

SELECT DATE_TRUNC('MICROSECONDS', time '15:14:13');
>> 15:14:13

SELECT DATE_TRUNC('microseconds', time '15:14:13');
>> 15:14:13

SELECT DATE_TRUNC('MICROSECONDS', time '15:14:13.123456789');
>> 15:14:13.123456

SELECT DATE_TRUNC('microseconds', time '15:14:13.123456789');
>> 15:14:13.123456

SELECT DATE_TRUNC('MICROSECONDS', date '2015-05-29');
>> 2015-05-29

SELECT DATE_TRUNC('microseconds', date '2015-05-29');
>> 2015-05-29

SELECT DATE_TRUNC('MICROSECONDS', date '1970-01-01');
>> 1970-01-01

SELECT DATE_TRUNC('microseconds', date '1970-01-01');
>> 1970-01-01

select DATE_TRUNC('MICROSECONDS', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2015-05-29 15:14:13+00

select DATE_TRUNC('microseconds', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2015-05-29 15:14:13+00

select DATE_TRUNC('MICROSECONDS', timestamp with time zone '2015-05-29 15:14:13.123456789+00');
>> 2015-05-29 15:14:13.123456+00

select DATE_TRUNC('microseconds', timestamp with time zone '2015-05-29 15:14:13.123456789+00');
>> 2015-05-29 15:14:13.123456+00

select DATE_TRUNC('MICROSECONDS', timestamp with time zone '2015-05-29 15:14:13-06');
>> 2015-05-29 15:14:13-06

select DATE_TRUNC('microseconds', timestamp with time zone '2015-05-29 15:14:13-06');
>> 2015-05-29 15:14:13-06

select DATE_TRUNC('MICROSECONDS', timestamp with time zone '2015-05-29 15:14:13.123456789-06');
>> 2015-05-29 15:14:13.123456-06

select DATE_TRUNC('microseconds', timestamp with time zone '2015-05-29 15:14:13.123456789-06');
>> 2015-05-29 15:14:13.123456-06

select DATE_TRUNC('MICROSECONDS', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 15:14:13+10

select DATE_TRUNC('microseconds', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 15:14:13+10

select DATE_TRUNC('MICROSECONDS', timestamp with time zone '2015-05-29 15:14:13.123456789+10');
>> 2015-05-29 15:14:13.123456+10

select DATE_TRUNC('microseconds', timestamp with time zone '2015-05-29 15:14:13.123456789+10');
>> 2015-05-29 15:14:13.123456+10

SELECT DATE_TRUNC('microseconds', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('MICROSECONDS', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('microseconds', timestamp '2015-05-29 15:14:13.123456789');
>> 2015-05-29 15:14:13.123456

SELECT DATE_TRUNC('MICROSECONDS', timestamp '2015-05-29 15:14:13.123456789');
>> 2015-05-29 15:14:13.123456

SELECT DATE_TRUNC('microseconds', timestamp '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('MICROSECONDS', timestamp '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('microseconds', timestamp '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('MICROSECONDS', timestamp '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

--
-- Test time unit in 'MILLISECONDS'
--
SELECT DATE_TRUNC('MILLISECONDS', time '00:00:00.000');
>> 00:00:00

SELECT DATE_TRUNC('milliseconds', time '00:00:00.000');
>> 00:00:00

SELECT DATE_TRUNC('MILLISECONDS', time '15:00:00');
>> 15:00:00

SELECT DATE_TRUNC('milliseconds', time '15:00:00');
>> 15:00:00

SELECT DATE_TRUNC('MILLISECONDS', time '15:14:13');
>> 15:14:13

SELECT DATE_TRUNC('milliseconds', time '15:14:13');
>> 15:14:13

SELECT DATE_TRUNC('MILLISECONDS', time '15:14:13.123456');
>> 15:14:13.123

SELECT DATE_TRUNC('milliseconds', time '15:14:13.123456');
>> 15:14:13.123

SELECT DATE_TRUNC('MILLISECONDS', date '2015-05-29');
>> 2015-05-29

SELECT DATE_TRUNC('milliseconds', date '2015-05-29');
>> 2015-05-29

SELECT DATE_TRUNC('MILLISECONDS', date '1970-01-01');
>> 1970-01-01

SELECT DATE_TRUNC('milliseconds', date '1970-01-01');
>> 1970-01-01

select DATE_TRUNC('MILLISECONDS', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2015-05-29 15:14:13+00

select DATE_TRUNC('milliseconds', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2015-05-29 15:14:13+00

select DATE_TRUNC('MILLISECONDS', timestamp with time zone '2015-05-29 15:14:13.123456+00');
>> 2015-05-29 15:14:13.123+00

select DATE_TRUNC('milliseconds', timestamp with time zone '2015-05-29 15:14:13.123456+00');
>> 2015-05-29 15:14:13.123+00

select DATE_TRUNC('MILLISECONDS', timestamp with time zone '2015-05-29 15:14:13-06');
>> 2015-05-29 15:14:13-06

select DATE_TRUNC('milliseconds', timestamp with time zone '2015-05-29 15:14:13-06');
>> 2015-05-29 15:14:13-06

select DATE_TRUNC('MILLISECONDS', timestamp with time zone '2015-05-29 15:14:13.123456-06');
>> 2015-05-29 15:14:13.123-06

select DATE_TRUNC('milliseconds', timestamp with time zone '2015-05-29 15:14:13.123456-06');
>> 2015-05-29 15:14:13.123-06

select DATE_TRUNC('MILLISECONDS', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 15:14:13+10

select DATE_TRUNC('milliseconds', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 15:14:13+10

select DATE_TRUNC('MILLISECONDS', timestamp with time zone '2015-05-29 15:14:13.123456+10');
>> 2015-05-29 15:14:13.123+10

select DATE_TRUNC('milliseconds', timestamp with time zone '2015-05-29 15:14:13.123456+10');
>> 2015-05-29 15:14:13.123+10

SELECT DATE_TRUNC('milliseconds', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('MILLISECONDS', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('milliseconds', timestamp '2015-05-29 15:14:13.123456');
>> 2015-05-29 15:14:13.123

SELECT DATE_TRUNC('MILLISECONDS', timestamp '2015-05-29 15:14:13.123456');
>> 2015-05-29 15:14:13.123

SELECT DATE_TRUNC('milliseconds', timestamp '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('MILLISECONDS', timestamp '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('milliseconds', timestamp '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('MILLISECONDS', timestamp '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

--
-- Test time unit 'SECOND'
--
SELECT DATE_TRUNC('SECOND', time '00:00:00.000');
>> 00:00:00

SELECT DATE_TRUNC('second', time '00:00:00.000');
>> 00:00:00

SELECT DATE_TRUNC('SECOND', time '15:00:00');
>> 15:00:00

SELECT DATE_TRUNC('second', time '15:00:00');
>> 15:00:00

SELECT DATE_TRUNC('SECOND', time '15:14:13');
>> 15:14:13

SELECT DATE_TRUNC('second', time '15:14:13');
>> 15:14:13

SELECT DATE_TRUNC('SECOND', time '15:14:13.123456');
>> 15:14:13

SELECT DATE_TRUNC('second', time '15:14:13.123456');
>> 15:14:13

SELECT DATE_TRUNC('SECOND', date '2015-05-29');
>> 2015-05-29

SELECT DATE_TRUNC('second', date '2015-05-29');
>> 2015-05-29

SELECT DATE_TRUNC('SECOND', date '1970-01-01');
>> 1970-01-01

SELECT DATE_TRUNC('second', date '1970-01-01');
>> 1970-01-01

select DATE_TRUNC('SECOND', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2015-05-29 15:14:13+00

select DATE_TRUNC('second', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2015-05-29 15:14:13+00

select DATE_TRUNC('SECOND', timestamp with time zone '2015-05-29 15:14:13.123456+00');
>> 2015-05-29 15:14:13+00

select DATE_TRUNC('second', timestamp with time zone '2015-05-29 15:14:13.123456+00');
>> 2015-05-29 15:14:13+00

select DATE_TRUNC('SECOND', timestamp with time zone '2015-05-29 15:14:13-06');
>> 2015-05-29 15:14:13-06

select DATE_TRUNC('second', timestamp with time zone '2015-05-29 15:14:13-06');
>> 2015-05-29 15:14:13-06

select DATE_TRUNC('SECOND', timestamp with time zone '2015-05-29 15:14:13.123456-06');
>> 2015-05-29 15:14:13-06

select DATE_TRUNC('second', timestamp with time zone '2015-05-29 15:14:13.123456-06');
>> 2015-05-29 15:14:13-06

select DATE_TRUNC('SECOND', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 15:14:13+10

select DATE_TRUNC('second', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 15:14:13+10

select DATE_TRUNC('SECOND', timestamp with time zone '2015-05-29 15:14:13.123456+10');
>> 2015-05-29 15:14:13+10

select DATE_TRUNC('second', timestamp with time zone '2015-05-29 15:14:13.123456+10');
>> 2015-05-29 15:14:13+10

SELECT DATE_TRUNC('second', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('SECOND', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('second', timestamp '2015-05-29 15:14:13.123456');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('SECOND', timestamp '2015-05-29 15:14:13.123456');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('second', timestamp '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('SECOND', timestamp '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('second', timestamp '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('SECOND', timestamp '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

--
-- Test time unit 'MINUTE'
--
SELECT DATE_TRUNC('MINUTE', time '00:00:00');
>> 00:00:00

SELECT DATE_TRUNC('minute', time '00:00:00');
>> 00:00:00

SELECT DATE_TRUNC('MINUTE', time '15:00:00');
>> 15:00:00

SELECT DATE_TRUNC('minute', time '15:00:00');
>> 15:00:00

SELECT DATE_TRUNC('MINUTE', time '15:14:13');
>> 15:14:00

SELECT DATE_TRUNC('minute', time '15:14:13');
>> 15:14:00

SELECT DATE_TRUNC('MINUTE', date '2015-05-29');
>> 2015-05-29

SELECT DATE_TRUNC('minute', date '2015-05-29');
>> 2015-05-29

SELECT DATE_TRUNC('MINUTE', date '1970-01-01');
>> 1970-01-01

SELECT DATE_TRUNC('minute', date '1970-01-01');
>> 1970-01-01

select DATE_TRUNC('MINUTE', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2015-05-29 15:14:00+00

select DATE_TRUNC('minute', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2015-05-29 15:14:00+00

select DATE_TRUNC('MINUTE', timestamp with time zone '2015-05-29 15:14:13-06');
>> 2015-05-29 15:14:00-06

select DATE_TRUNC('minute', timestamp with time zone '2015-05-29 15:14:13-06');
>> 2015-05-29 15:14:00-06

select DATE_TRUNC('MINUTE', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 15:14:00+10

select DATE_TRUNC('minute', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 15:14:00+10

SELECT DATE_TRUNC('MINUTE', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 15:14:00

SELECT DATE_TRUNC('MINUTE', timestamp '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('MINUTE', timestamp '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

--
-- Test time unit 'HOUR'
--
SELECT DATE_TRUNC('HOUR', time '00:00:00');
>> 00:00:00

SELECT DATE_TRUNC('hour', time '00:00:00');
>> 00:00:00

SELECT DATE_TRUNC('HOUR', time '15:00:00');
>> 15:00:00

SELECT DATE_TRUNC('hour', time '15:00:00');
>> 15:00:00

SELECT DATE_TRUNC('HOUR', time '15:14:13');
>> 15:00:00

SELECT DATE_TRUNC('hour', time '15:14:13');
>> 15:00:00

SELECT DATE_TRUNC('HOUR', date '2015-05-29');
>> 2015-05-29

SELECT DATE_TRUNC('hour', date '2015-05-29');
>> 2015-05-29

SELECT DATE_TRUNC('HOUR', date '1970-01-01');
>> 1970-01-01

SELECT DATE_TRUNC('hour', date '1970-01-01');
>> 1970-01-01

select DATE_TRUNC('HOUR', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2015-05-29 15:00:00+00

select DATE_TRUNC('hour', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2015-05-29 15:00:00+00

select DATE_TRUNC('HOUR', timestamp with time zone '2015-05-29 15:14:13-06');
>> 2015-05-29 15:00:00-06

select DATE_TRUNC('hour', timestamp with time zone '2015-05-29 15:14:13-06');
>> 2015-05-29 15:00:00-06

select DATE_TRUNC('HOUR', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 15:00:00+10

select DATE_TRUNC('hour', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 15:00:00+10

SELECT DATE_TRUNC('hour', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('HOUR', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('hour', timestamp '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('HOUR', timestamp '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('hour', timestamp '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('HOUR', timestamp '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

--
-- Test time unit 'DAY'
--
select DATE_TRUNC('day', time '00:00:00');
>> 00:00:00

select DATE_TRUNC('DAY', time '00:00:00');
>> 00:00:00

select DATE_TRUNC('day', time '15:14:13');
>> 00:00:00

select DATE_TRUNC('DAY', time '15:14:13');
>> 00:00:00

select DATE_TRUNC('day', date '2015-05-29');
>> 2015-05-29

select DATE_TRUNC('DAY', date '2015-05-29');
>> 2015-05-29

select DATE_TRUNC('day', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 00:00:00

select DATE_TRUNC('DAY', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 00:00:00

select DATE_TRUNC('day', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2015-05-29 00:00:00+00

select DATE_TRUNC('DAY', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2015-05-29 00:00:00+00

select DATE_TRUNC('day', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-05-29 00:00:00-06

select DATE_TRUNC('DAY', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-05-29 00:00:00-06

select DATE_TRUNC('day', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 00:00:00+10

select DATE_TRUNC('DAY', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 00:00:00+10

--
-- Test time unit 'WEEK'
--
select DATE_TRUNC('week', time '00:00:00');
>> 00:00:00

select DATE_TRUNC('WEEK', time '00:00:00');
>> 00:00:00

select DATE_TRUNC('week', time '15:14:13');
>> 00:00:00

select DATE_TRUNC('WEEK', time '15:14:13');
>> 00:00:00

-- ISO_WEEK

SELECT DATE_TRUNC(ISO_WEEK, TIME '00:00:00');
>> 00:00:00

SELECT DATE_TRUNC(ISO_WEEK, TIME '15:14:13');
>> 00:00:00

SELECT DATE_TRUNC(ISO_WEEK, DATE '2015-05-28');
>> 2015-05-25

SELECT DATE_TRUNC(ISO_WEEK, TIMESTAMP '2015-05-29 15:14:13');
>> 2015-05-25 00:00:00

SELECT DATE_TRUNC(ISO_WEEK, TIMESTAMP '2018-03-14 00:00:00.000');
>> 2018-03-12 00:00:00

SELECT DATE_TRUNC(ISO_WEEK, TIMESTAMP WITH TIME ZONE '2015-05-29 15:14:13+00');
>> 2015-05-25 00:00:00+00

SELECT DATE_TRUNC(ISO_WEEK, TIMESTAMP WITH TIME ZONE '2015-05-29 05:14:13-06');
>> 2015-05-25 00:00:00-06

SELECT DATE_TRUNC(ISO_WEEK, TIMESTAMP WITH TIME ZONE '2015-05-29 15:14:13+10');
>> 2015-05-25 00:00:00+10

--
-- Test time unit 'MONTH'
--
select DATE_TRUNC('month', time '00:00:00');
>> 00:00:00

select DATE_TRUNC('MONTH', time '00:00:00');
>> 00:00:00

select DATE_TRUNC(MONTH, time '00:00:00');
>> 00:00:00

select DATE_TRUNC('month', time '15:14:13');
>> 00:00:00

select DATE_TRUNC('MONTH', time '15:14:13');
>> 00:00:00

select DATE_TRUNC('month', date '2015-05-28');
>> 2015-05-01

select DATE_TRUNC('MONTH', date '2015-05-28');
>> 2015-05-01

select DATE_TRUNC('month', timestamp '2015-05-29 15:14:13');
>> 2015-05-01 00:00:00

select DATE_TRUNC('MONTH', timestamp '2015-05-29 15:14:13');
>> 2015-05-01 00:00:00

SELECT DATE_TRUNC('MONTH', timestamp '2018-03-14 00:00:00.000');
>> 2018-03-01 00:00:00

select DATE_TRUNC('month', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2015-05-01 00:00:00+00

select DATE_TRUNC('MONTH', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2015-05-01 00:00:00+00

select DATE_TRUNC('month', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-05-01 00:00:00-06

select DATE_TRUNC('MONTH', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-05-01 00:00:00-06

select DATE_TRUNC('month', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-01 00:00:00+10

select DATE_TRUNC('MONTH', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-01 00:00:00+10

--
-- Test time unit 'QUARTER'
--
select DATE_TRUNC('quarter', time '00:00:00');
>> 00:00:00

select DATE_TRUNC('QUARTER', time '00:00:00');
>> 00:00:00

select DATE_TRUNC('quarter', time '15:14:13');
>> 00:00:00

select DATE_TRUNC('QUARTER', time '15:14:13');
>> 00:00:00

select DATE_TRUNC('quarter', date '2015-05-28');
>> 2015-04-01

select DATE_TRUNC('QUARTER', date '2015-05-28');
>> 2015-04-01

select DATE_TRUNC('quarter', timestamp '2015-05-29 15:14:13');
>> 2015-04-01 00:00:00

select DATE_TRUNC('QUARTER', timestamp '2015-05-29 15:14:13');
>> 2015-04-01 00:00:00

SELECT DATE_TRUNC('QUARTER', timestamp '2018-03-14 00:00:00.000');
>> 2018-01-01 00:00:00

SELECT DATE_TRUNC('QUARTER', timestamp '2015-05-29 15:14:13');
>> 2015-04-01 00:00:00

SELECT DATE_TRUNC('QUARTER', timestamp '2015-05-01 15:14:13');
>> 2015-04-01 00:00:00

SELECT DATE_TRUNC('QUARTER', timestamp '2015-07-29 15:14:13');
>> 2015-07-01 00:00:00

SELECT DATE_TRUNC('QUARTER', timestamp '2015-09-29 15:14:13');
>> 2015-07-01 00:00:00

SELECT DATE_TRUNC('QUARTER', timestamp '2015-10-29 15:14:13');
>> 2015-10-01 00:00:00

SELECT DATE_TRUNC('QUARTER', timestamp '2015-12-29 15:14:13');
>> 2015-10-01 00:00:00

select DATE_TRUNC('quarter', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2015-04-01 00:00:00+00

select DATE_TRUNC('QUARTER', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2015-04-01 00:00:00+00

select DATE_TRUNC('quarter', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-04-01 00:00:00-06

select DATE_TRUNC('QUARTER', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-04-01 00:00:00-06

select DATE_TRUNC('quarter', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-04-01 00:00:00+10

select DATE_TRUNC('QUARTER', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-04-01 00:00:00+10

--
-- Test time unit 'YEAR'
--
select DATE_TRUNC('year', time '00:00:00');
>> 00:00:00

select DATE_TRUNC('YEAR', time '00:00:00');
>> 00:00:00

select DATE_TRUNC('year', time '15:14:13');
>> 00:00:00

select DATE_TRUNC('YEAR', time '15:14:13');
>> 00:00:00

select DATE_TRUNC('year', date '2015-05-28');
>> 2015-01-01

select DATE_TRUNC('YEAR', date '2015-05-28');
>> 2015-01-01

select DATE_TRUNC('year', timestamp '2015-05-29 15:14:13');
>> 2015-01-01 00:00:00

select DATE_TRUNC('YEAR', timestamp '2015-05-29 15:14:13');
>> 2015-01-01 00:00:00

select DATE_TRUNC('year', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2015-01-01 00:00:00+00

select DATE_TRUNC('YEAR', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2015-01-01 00:00:00+00

select DATE_TRUNC('year', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-01-01 00:00:00-06

select DATE_TRUNC('YEAR', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-01-01 00:00:00-06

select DATE_TRUNC('year', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-01-01 00:00:00+10

select DATE_TRUNC('YEAR', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-01-01 00:00:00+10

--
-- Test time unit 'DECADE'
--
select DATE_TRUNC('decade', time '00:00:00');
>> 00:00:00

select DATE_TRUNC('DECADE', time '00:00:00');
>> 00:00:00

select DATE_TRUNC('decade', time '15:14:13');
>> 00:00:00

select DATE_TRUNC('DECADE', time '15:14:13');
>> 00:00:00

select DATE_TRUNC('decade', date '2015-05-28');
>> 2010-01-01

select DATE_TRUNC('DECADE', date '2015-05-28');
>> 2010-01-01

select DATE_TRUNC('decade', timestamp '2015-05-29 15:14:13');
>> 2010-01-01 00:00:00

select DATE_TRUNC('DECADE', timestamp '2015-05-29 15:14:13');
>> 2010-01-01 00:00:00

SELECT DATE_TRUNC('decade', timestamp '2010-05-29 15:14:13');
>> 2010-01-01 00:00:00

select DATE_TRUNC('decade', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2010-01-01 00:00:00+00

select DATE_TRUNC('DECADE', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2010-01-01 00:00:00+00

select DATE_TRUNC('decade', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2010-01-01 00:00:00-06

select DATE_TRUNC('DECADE', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2010-01-01 00:00:00-06

select DATE_TRUNC('decade', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2010-01-01 00:00:00+10

select DATE_TRUNC('DECADE', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2010-01-01 00:00:00+10

--
-- Test time unit 'CENTURY'
--
select DATE_TRUNC('century', time '00:00:00');
>> 00:00:00

select DATE_TRUNC('CENTURY', time '00:00:00');
>> 00:00:00

select DATE_TRUNC('century', time '15:14:13');
>> 00:00:00

select DATE_TRUNC('CENTURY', time '15:14:13');
>> 00:00:00

select DATE_TRUNC('century', date '2015-05-28');
>> 2001-01-01

select DATE_TRUNC('CENTURY', date '2015-05-28');
>> 2001-01-01

select DATE_TRUNC('century', timestamp '2015-05-29 15:14:13');
>> 2001-01-01 00:00:00

select DATE_TRUNC('CENTURY', timestamp '2015-05-29 15:14:13');
>> 2001-01-01 00:00:00

SELECT DATE_TRUNC('century', timestamp '2199-05-29 15:14:13');
>> 2101-01-01 00:00:00

SELECT DATE_TRUNC('CENTURY', timestamp '2000-05-29 15:14:13');
>> 1901-01-01 00:00:00

SELECT DATE_TRUNC('century', timestamp '2001-05-29 15:14:13');
>> 2001-01-01 00:00:00

select DATE_TRUNC('century', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2001-01-01 00:00:00+00

select DATE_TRUNC('CENTURY', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2001-01-01 00:00:00+00

select DATE_TRUNC('century', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2001-01-01 00:00:00-06

select DATE_TRUNC('CENTURY', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2001-01-01 00:00:00-06

select DATE_TRUNC('century', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2001-01-01 00:00:00+10

select DATE_TRUNC('CENTURY', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2001-01-01 00:00:00+10

--
-- Test time unit 'MILLENNIUM'
--
select DATE_TRUNC('millennium', time '00:00:00');
>> 00:00:00

select DATE_TRUNC('MILLENNIUM', time '00:00:00');
>> 00:00:00

select DATE_TRUNC('millennium', time '15:14:13');
>> 00:00:00

select DATE_TRUNC('MILLENNIUM', time '15:14:13');
>> 00:00:00

select DATE_TRUNC('millennium', date '2015-05-28');
>> 2001-01-01

select DATE_TRUNC('MILLENNIUM', date '2015-05-28');
>> 2001-01-01

select DATE_TRUNC('millennium', timestamp '2015-05-29 15:14:13');
>> 2001-01-01 00:00:00

select DATE_TRUNC('MILLENNIUM', timestamp '2015-05-29 15:14:13');
>> 2001-01-01 00:00:00

SELECT DATE_TRUNC('millennium', timestamp '2000-05-29 15:14:13');
>> 1001-01-01 00:00:00

select DATE_TRUNC('millennium', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2001-01-01 00:00:00+00

select DATE_TRUNC('MILLENNIUM', timestamp with time zone '2015-05-29 15:14:13+00');
>> 2001-01-01 00:00:00+00

select DATE_TRUNC('millennium', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2001-01-01 00:00:00-06

select DATE_TRUNC('MILLENNIUM', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2001-01-01 00:00:00-06

select DATE_TRUNC('millennium', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2001-01-01 00:00:00+10

select DATE_TRUNC('MILLENNIUM', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2001-01-01 00:00:00+10

--
-- Test unhandled time unit and bad date
--
SELECT DATE_TRUNC('---', '2015-05-29 15:14:13');
> exception INVALID_VALUE_2

SELECT DATE_TRUNC('', '2015-05-29 15:14:13');
> exception INVALID_VALUE_2

SELECT DATE_TRUNC('', '');
> exception INVALID_VALUE_2

SELECT DATE_TRUNC('YEAR', '');
> exception INVALID_VALUE_2

SELECT DATE_TRUNC('microseconds', '2015-05-29 15:14:13');
> exception INVALID_VALUE_2

SET MODE PostgreSQL;
> ok

select DATE_TRUNC('YEAR', DATE '2015-05-28');
>> 2015-01-01 00:00:00+01

SET MODE Regular;
> ok

SELECT DATE_TRUNC(DECADE, DATE '0000-01-20');
>> 0000-01-01

SELECT DATE_TRUNC(DECADE, DATE '-1-12-31');
>> -0010-01-01

SELECT DATE_TRUNC(DECADE, DATE '-10-01-01');
>> -0010-01-01

SELECT DATE_TRUNC(DECADE, DATE '-11-12-31');
>> -0020-01-01

SELECT DATE_TRUNC(CENTURY, DATE '0001-01-20');
>> 0001-01-01

SELECT DATE_TRUNC(CENTURY, DATE '0000-12-31');
>> -0099-01-01

SELECT DATE_TRUNC(CENTURY, DATE '-1-12-31');
>> -0099-01-01

SELECT DATE_TRUNC(CENTURY, DATE '-99-01-01');
>> -0099-01-01

SELECT DATE_TRUNC(CENTURY, DATE '-100-12-31');
>> -0199-01-01

SELECT DATE_TRUNC(MILLENNIUM, DATE '0001-01-20');
>> 0001-01-01

SELECT DATE_TRUNC(MILLENNIUM, DATE '0000-12-31');
>> -0999-01-01

SELECT DATE_TRUNC(MILLENNIUM, DATE '-1-12-31');
>> -0999-01-01

SELECT DATE_TRUNC(MILLENNIUM, DATE '-999-01-01');
>> -0999-01-01

SELECT DATE_TRUNC(MILLENNIUM, DATE '-1000-12-31');
>> -1999-01-01

-- ISO_WEEK_YEAR

SELECT DATE_TRUNC(ISO_WEEK_YEAR, DATE '2019-12-30');
>> 2019-12-30

SELECT DATE_TRUNC(ISO_WEEK_YEAR, DATE '2020-01-01');
>> 2019-12-30

SELECT DATE_TRUNC(ISO_WEEK_YEAR, DATE '2020-12-01');
>> 2019-12-30

SELECT DATE_TRUNC(ISO_WEEK_YEAR, DATE '2020-12-31');
>> 2019-12-30

SELECT DATE_TRUNC(ISO_WEEK_YEAR, DATE '2017-01-01');
>> 2016-01-04

SELECT DATE_TRUNC(ISO_WEEK_YEAR, DATE '2017-01-02');
>> 2017-01-02
