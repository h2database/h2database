-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT CARDINALITY(NULL);
>> null

SELECT CARDINALITY(ARRAY[]);
>> 0

SELECT CARDINALITY(ARRAY[1, 2, 5]);
>> 3

SELECT ARRAY_LENGTH(ARRAY[1, 2, 5]);
>> 3
