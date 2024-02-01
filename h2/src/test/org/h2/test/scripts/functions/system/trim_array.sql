-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT TRIM_ARRAY(ARRAY[1, 2], -1);
> exception ARRAY_ELEMENT_ERROR_2

SELECT TRIM_ARRAY(ARRAY[1, 2], 0);
>> [1, 2]

SELECT TRIM_ARRAY(ARRAY[1, 2], 1);
>> [1]

SELECT TRIM_ARRAY(ARRAY[1, 2], 2);
>> []

SELECT TRIM_ARRAY(ARRAY[1, 2], 3);
> exception ARRAY_ELEMENT_ERROR_2

SELECT TRIM_ARRAY(NULL, 1);
>> null

SELECT TRIM_ARRAY(NULL, -1);
> exception ARRAY_ELEMENT_ERROR_2

SELECT TRIM_ARRAY(ARRAY[1], NULL);
>> null
