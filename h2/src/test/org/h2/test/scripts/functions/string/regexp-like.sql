-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

call select 1 from dual where regexp_like('x', 'x', '\');
> exception INVALID_VALUE_2

CALL REGEXP_LIKE('A', '[a-z]', 'i');
>> TRUE

CALL REGEXP_LIKE('A', '[a-z]', 'c');
>> FALSE
