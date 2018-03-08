select DATE_TRUNC('day', time '00:00:00');
>> 00:00:00

select DATE_TRUNC('day', time '15:14:13');
>> 00:00:00

select DATE_TRUNC('day', date '2015-05-29');
>> 2015-05-29

select DATE_TRUNC('day', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 00:00:00

select DATE_TRUNC('day', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-05-29 00:00:00+00

select DATE_TRUNC('day', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-05-29 00:00:00-06

select DATE_TRUNC('day', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 00:00:00+10

select DATE_TRUNC('day', '2015-05-29 15:14:13');
>> 2015-05-29 00:00:00
