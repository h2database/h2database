-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

set mode PostgreSQL;
> ok

select array_to_string(array[null, 0, 1, null, 2], ',');
>> 0,1,2

select array_to_string(array['a', null, '', 'b', null], ',', null);
>> a,,b

select array_to_string(array[null, 0, 1, null, 2], ',', '*');
>> *,0,1,*,2

select array_to_string(array['a', null, '', 'b', null], ',', '*');
>> a,*,,b,*

select array_to_string(array[1, null, 3], 0, 2);
>> 10203

select array_to_string(null, 0, 2);
>> null

select array_to_string(array[1, null, 3], null, 2);
>> null

select array_to_string(0, ',');
> exception INVALID_VALUE_2

set mode Regular;
> ok
