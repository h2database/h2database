-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select array_slice(ARRAY[1, 2, 3, 4], 1, 1) = ARRAY[1];
>> TRUE

select array_slice(ARRAY[1, 2, 3, 4], 1, 3) = ARRAY[1, 2, 3];
>> TRUE

select array_slice(ARRAY[1, 2, 3, 4], 0, 3) is null;
>> TRUE

select array_slice(ARRAY[1, 2, 3, 4], 1, 5) is null;
>> TRUE
