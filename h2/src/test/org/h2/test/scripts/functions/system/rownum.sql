-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

----- Issue#600 -----
create table test as (select char(x) as str from system_range(48,90));
> ok

select rownum() as rnum, str from test where str = 'A';
> RNUM STR
> ---- ---
> 1    A
> rows: 1

----- Issue#3353 -----
SELECT str FROM FINAL TABLE (UPDATE test SET str = char(rownum + 48) WHERE str = '0');
> STR
> ---
> 1
> rows: 1

drop table test;
> ok

SELECT * FROM (VALUES 1, 2) AS T1(X), (VALUES 1, 2) AS T2(X) WHERE ROWNUM = 1;
> X X
> - -
> 1 1
> rows: 1

SELECT 1 ORDER BY ROWNUM;
>> 1
