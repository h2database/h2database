-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT SELECTIVITY(X) FROM (VALUES 1, 2, 3, 3, NULL, NULL) T(X);
>> 66

SELECT SELECTIVITY(X) FROM (VALUES 1, 2, 3, 3, NULL, NULL, 6, 7, 8, 9) T(X);
>> 80

SELECT SELECTIVITY(X) FROM (VALUES 1, 2, 3) T(X);
>> 100

SELECT SELECTIVITY(X) FROM (VALUES 1, 1, 1, 1) T(X);
>> 25

SELECT SELECTIVITY(DISTINCT X) FROM (VALUES 1) T(X);
> exception SYNTAX_ERROR_2

SELECT SELECTIVITY(ALL X) FROM (VALUES 1) T(X);
> exception SYNTAX_ERROR_2
