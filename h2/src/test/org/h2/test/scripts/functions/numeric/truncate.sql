-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT TRUNCATE(1.234, 2);
>> 1.23

SELECT (CURRENT_TIMESTAMP - CURRENT_TIME(6)) = TRUNCATE(CURRENT_TIMESTAMP);
>> TRUE

SELECT TRUNCATE('bad', 1);
> exception INVALID_DATETIME_CONSTANT_2

SELECT TRUNCATE(1, 2, 3);
> exception INVALID_PARAMETER_COUNT_2

select truncate(null, null) en, truncate(1.99, 0) e1, truncate(-10.9, 0) em10;
> EN   E1  EM10
> ---- --- -----
> null 1.0 -10.0
> rows: 1

select trunc(null, null) en, trunc(1.99, 0) e1, trunc(-10.9, 0) em10;
> EN   E1  EM10
> ---- --- -----
> null 1.0 -10.0
> rows: 1

select trunc(1.3);
>> 1.0
