-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT (10, 20, 30)[1];
>> 10

SELECT (10, 20, 30)[3];
>> 30

SELECT (10, 20, 30)[0];
>> null

SELECT (10, 20, 30)[4];
>> null

SELECT ARRAY[];
>> []

SELECT ARRAY[10];
>> [10]

SELECT ARRAY[10, 20, 30];
>> [10, 20, 30]
