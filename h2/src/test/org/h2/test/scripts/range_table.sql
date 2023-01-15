-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

explain select * from system_range(1, 2) where x=x+1 and x=1;
>> SELECT "SYSTEM_RANGE"."X" FROM SYSTEM_RANGE(1, 2) /* range index: X = CAST(1 AS BIGINT) */ WHERE ("X" = CAST(1 AS BIGINT)) AND ("X" = ("X" + 1))

explain select * from system_range(1, 2) where not (x = 1 and x*2 = 2);
>> SELECT "SYSTEM_RANGE"."X" FROM SYSTEM_RANGE(1, 2) /* range index */ WHERE ("X" <> CAST(1 AS BIGINT)) OR (("X" * 2) <> 2)

explain select * from system_range(1, 10) where (NOT x >= 5);
>> SELECT "SYSTEM_RANGE"."X" FROM SYSTEM_RANGE(1, 10) /* range index: X < CAST(5 AS BIGINT) */ WHERE "X" < CAST(5 AS BIGINT)

select (select t1.x from system_range(1,1) t2) from system_range(1,1) t1;
> (SELECT T1.X FROM SYSTEM_RANGE(1, 1) T2)
> ----------------------------------------
> 1
> rows: 1

EXPLAIN PLAN FOR SELECT * FROM SYSTEM_RANGE(1, 20);
>> SELECT "SYSTEM_RANGE"."X" FROM SYSTEM_RANGE(1, 20) /* range index */

select sum(x) from system_range(2, 1000) r where
not exists(select * from system_range(2, 32) r2 where r.x>r2.x and mod(r.x, r2.x)=0);
>> 76127

SELECT COUNT(*) FROM SYSTEM_RANGE(0, 2111222333);
>> 2111222334

select * from system_range(2, 100) r where
not exists(select * from system_range(2, 11) r2 where r.x>r2.x and mod(r.x, r2.x)=0);
> X
> --
> 11
> 13
> 17
> 19
> 2
> 23
> 29
> 3
> 31
> 37
> 41
> 43
> 47
> 5
> 53
> 59
> 61
> 67
> 7
> 71
> 73
> 79
> 83
> 89
> 97
> rows: 25

SELECT * FROM SYSTEM_RANGE(1, 10) ORDER BY 1;
> X
> --
> 1
> 2
> 3
> 4
> 5
> 6
> 7
> 8
> 9
> 10
> rows (ordered): 10

SELECT COUNT(*) FROM SYSTEM_RANGE(1, 10);
>> 10

SELECT * FROM SYSTEM_RANGE(1, 10, 2) ORDER BY 1;
> X
> -
> 1
> 3
> 5
> 7
> 9
> rows (ordered): 5

SELECT COUNT(*) FROM SYSTEM_RANGE(1, 10, 2);
>> 5

SELECT * FROM SYSTEM_RANGE(1, 9, 2) ORDER BY 1;
> X
> -
> 1
> 3
> 5
> 7
> 9
> rows (ordered): 5

SELECT COUNT(*) FROM SYSTEM_RANGE(1, 9, 2);
>> 5

SELECT * FROM SYSTEM_RANGE(10, 1, -2) ORDER BY 1 DESC;
> X
> --
> 10
> 8
> 6
> 4
> 2
> rows (ordered): 5

SELECT COUNT(*) FROM SYSTEM_RANGE(10, 1, -2);
>> 5

SELECT * FROM SYSTEM_RANGE(10, 2, -2) ORDER BY 1 DESC;
> X
> --
> 10
> 8
> 6
> 4
> 2
> rows (ordered): 5

SELECT COUNT(*) FROM SYSTEM_RANGE(10, 2, -2);
>> 5

SELECT * FROM SYSTEM_RANGE(1, 1);
>> 1

SELECT COUNT(*) FROM SYSTEM_RANGE(1, 1);
>> 1

SELECT * FROM SYSTEM_RANGE(1, 1, -1);
>> 1

SELECT COUNT(*) FROM SYSTEM_RANGE(1, 1, -1);
>> 1

SELECT * FROM SYSTEM_RANGE(2, 1);
> X
> -
> rows: 0

SELECT COUNT(*) FROM SYSTEM_RANGE(2, 1);
>> 0

SELECT * FROM SYSTEM_RANGE(2, 1, 2);
> X
> -
> rows: 0

SELECT COUNT(*) FROM SYSTEM_RANGE(2, 1, 2);
>> 0

SELECT * FROM SYSTEM_RANGE(1, 2, 0);
> exception STEP_SIZE_MUST_NOT_BE_ZERO

SELECT COUNT(*) FROM SYSTEM_RANGE(1, 2, 0);
> exception STEP_SIZE_MUST_NOT_BE_ZERO

SELECT * FROM SYSTEM_RANGE(2, 1, 0);
> exception STEP_SIZE_MUST_NOT_BE_ZERO

SELECT COUNT(*) FROM SYSTEM_RANGE(2, 1, 0);
> exception STEP_SIZE_MUST_NOT_BE_ZERO

SELECT * FROM SYSTEM_RANGE(1, 8, 2);
> X
> -
> 1
> 3
> 5
> 7
> rows: 4

SELECT * FROM SYSTEM_RANGE(1, 8, 2) WHERE X = 2;
> X
> -
> rows: 0

SELECT COUNT(*) FROM SYSTEM_RANGE(1, 8, 2) WHERE X = 2;
>> 0

SELECT * FROM SYSTEM_RANGE(1, 8, 2) WHERE X BETWEEN 2 AND 6;
> X
> -
> 3
> 5
> rows: 2

SELECT COUNT(*) FROM SYSTEM_RANGE(1, 8, 2) WHERE X BETWEEN 2 AND 6;
>> 2

SELECT * FROM SYSTEM_RANGE(8, 1, -2) ORDER BY X DESC;
> X
> -
> 8
> 6
> 4
> 2
> rows (ordered): 4

SELECT * FROM SYSTEM_RANGE(8, 1, -2) WHERE X = 3;
> X
> -
> rows: 0

SELECT COUNT(*) FROM SYSTEM_RANGE(8, 1, -2) WHERE X = 3;
>> 0

SELECT * FROM SYSTEM_RANGE(8, 1, -2) WHERE X BETWEEN 3 AND 7 ORDER BY 1 DESC;
> X
> -
> 6
> 4
> rows (ordered): 2

SELECT COUNT(*) FROM SYSTEM_RANGE(8, 1, -2) WHERE X BETWEEN 3 AND 7;
>> 2

SELECT X FROM SYSTEM_RANGE(1, 2, ?);
{
1
> X
> -
> 1
> 2
> rows: 2
};
> update count: 0
