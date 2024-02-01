-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(A INTEGER ARRAY) AS VALUES ARRAY[NULL], ARRAY[1];
> ok

SELECT A, ARRAY_GET(A, 1), ARRAY_GET(A, 1) IS OF (INTEGER) FROM TEST;
> A      A[1] A[1] IS OF (INTEGER)
> ------ ---- --------------------
> [1]    1    TRUE
> [null] null null
> rows: 2

DROP TABLE TEST;
> ok
