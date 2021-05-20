-- Copyright 2021-2021 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- Test the limits for temporary group by in memory structures

CREATE TABLE TEST (A INT, B VARCHAR) AS (VALUES (10, 'STRING 1'), (11, 'STRING 2'));
> ok

SELECT A, count(*) from TEST group by A;
> A  COUNT(*)
> -- --------
> 10 1
> 11 1
> rows: 2

SELECT B, count(*) from TEST WHERE A<11 group by B;
> B        COUNT(*)
> -------- --------
> STRING 1 1
> rows: 1

SELECT B, count(*) from TEST group by B;
> exception GROUP_BY_TABLE_TOO_LARGE

SELECT A, max(B) from TEST group by A;
> A  MAX(B)
> -- --------
> 10 STRING 1
> 11 STRING 2
> rows: 2