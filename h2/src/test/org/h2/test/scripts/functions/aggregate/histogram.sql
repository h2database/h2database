-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT HISTOGRAM(X), FROM VALUES (1), (2), (3), (1), (2), (NULL), (5) T(X);
>> [[null, 1], [1, 2], [2, 2], [3, 1], [5, 1]]

SELECT HISTOGRAM(X) FILTER (WHERE X > 1) FROM VALUES (1), (2), (3), (1), (2), (NULL), (5) T(X);
>> [[2, 2], [3, 1], [5, 1]]

SELECT HISTOGRAM(X) FILTER (WHERE X > 0) FROM VALUES (0) T(X);
>> []

SELECT HISTOGRAM(DISTINCT X) FROM VALUES (0) T(X);
> exception SYNTAX_ERROR_2

SELECT HISTOGRAM(ALL X) FROM VALUES (0) T(X);
> exception SYNTAX_ERROR_2
