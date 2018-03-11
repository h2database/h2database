
--
-- Test time unit 'HOUR'
--

SELECT DATE_TRUNC('HOUR', time '00:00:00');
>> 1970-01-01 00:00:00

SELECT DATE_TRUNC('hour', time '00:00:00');
>> 1970-01-01 00:00:00

SELECT DATE_TRUNC('HOUR', time '15:00:00');
>> 1970-01-01 15:00:00

SELECT DATE_TRUNC('hour', time '15:00:00');
>> 1970-01-01 15:00:00

SELECT DATE_TRUNC('HOUR', time '15:14:13');
>> 1970-01-01 15:00:00

SELECT DATE_TRUNC('hour', time '15:14:13');
>> 1970-01-01 15:00:00

SELECT DATE_TRUNC('HOUR', date '2015-05-29');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('hour', date '2015-05-29');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('HOUR', date '1970-01-01');
>> 1970-01-01 00:00:00

SELECT DATE_TRUNC('hour', date '1970-01-01');
>> 1970-01-01 00:00:00

select DATE_TRUNC('HOUR', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-05-29 15:00:00+00

select DATE_TRUNC('hour', timestamp with time zone '2015-05-29 15:14:13');
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

SELECT DATE_TRUNC('hour', '2015-05-29 15:14:13');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('HOUR', '2015-05-29 15:14:13');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('hour', '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('HOUR', '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('hour', '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('HOUR', '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00


-- 
-- Test time unit 'DAY'
--
select DATE_TRUNC('day', time '00:00:00');
>> 1970-01-01 00:00:00

select DATE_TRUNC('DAY', time '00:00:00');
>> 1970-01-01 00:00:00

select DATE_TRUNC('day', time '15:14:13');
>> 1970-01-01 00:00:00

select DATE_TRUNC('DAY', time '15:14:13');
>> 1970-01-01 00:00:00

select DATE_TRUNC('day', date '2015-05-29');
>> 2015-05-29 00:00:00

select DATE_TRUNC('DAY', date '2015-05-29');
>> 2015-05-29 00:00:00

select DATE_TRUNC('day', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 00:00:00

select DATE_TRUNC('DAY', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 00:00:00

select DATE_TRUNC('day', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-05-29 00:00:00+00

select DATE_TRUNC('DAY', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-05-29 00:00:00+00

select DATE_TRUNC('day', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-05-29 00:00:00-06

select DATE_TRUNC('DAY', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-05-29 00:00:00-06

select DATE_TRUNC('day', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 00:00:00+10

select DATE_TRUNC('DAY', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 00:00:00+10

select DATE_TRUNC('day', '2015-05-29 15:14:13');
>> 2015-05-29 00:00:00

select DATE_TRUNC('DAY', '2015-05-29 15:14:13');
>> 2015-05-29 00:00:00


--
-- Test unhandled time unit
--
SELECT DATE_TRUNC('---', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('microseconds', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('MICROSECONDS', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('milliseconds', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('MILLISECONDS', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('second', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('SECOND', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('minute', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('MINUTE', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('week', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('WEEK', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('month', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('MONTH', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('quarter', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('QUARTER', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('year', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('YEAR', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('decade', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('DECADE', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('century', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('CENTURY', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('millennium', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('MILLENNIUM', '2015-05-29 15:14:13');
> exception
