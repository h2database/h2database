-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select case when 1=null then 1 else 2 end;
>> 2

select case (1) when 1 then 1 else 2 end;
>> 1

select x, case when x=0 then 'zero' else 'not zero' end y from system_range(0, 2);
> X Y
> - --------
> 0 zero
> 1 not zero
> 2 not zero
> rows: 3

select x, case when x=0 then 'zero' end y from system_range(0, 1);
> X Y
> - ----
> 0 zero
> 1 null
> rows: 2

select x, case x when 0 then 'zero' else 'not zero' end y from system_range(0, 1);
> X Y
> - --------
> 0 zero
> 1 not zero
> rows: 2

select x, case x when 0 then 'zero' when 1 then 'one' end y from system_range(0, 2);
> X Y
> - ----
> 0 zero
> 1 one
> 2 null
> rows: 3

SELECT X, CASE X WHEN 1 THEN 10 WHEN 2, 3 THEN 25 WHEN 4, 5, 6 THEN 50 ELSE 90 END C FROM SYSTEM_RANGE(1, 7);
> X C
> - --
> 1 10
> 2 25
> 3 25
> 4 50
> 5 50
> 6 50
> 7 90
> rows: 7

SELECT CASE WHEN TRUE THEN 1 END CASE;
> exception SYNTAX_ERROR_1

SELECT S, CASE S
    WHEN IS NULL THEN 1
    WHEN LOWER('A') THEN 2
    WHEN LIKE '%b' THEN 3
    WHEN ILIKE 'C' THEN 4
    WHEN REGEXP '[dQ]' THEN 5
    WHEN IS NOT DISTINCT FROM 'e' THEN 6
    WHEN IN ('x', 'f') THEN 7
    WHEN IN (VALUES 'g', 'z') THEN 8
    WHEN BETWEEN 'h' AND 'i' THEN 9
    WHEN = 'j' THEN 10
    WHEN < ANY(VALUES 'j', 'l') THEN 11
    WHEN NOT LIKE '%m%' THEN 12
    WHEN IS OF (VARCHAR) THEN 13
    ELSE 13
    END FROM (VALUES NULL, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm') T(S);
> S    C2
> ---- --
> a    2
> b    3
> c    4
> d    5
> e    6
> f    7
> g    8
> h    9
> i    9
> j    10
> k    11
> l    12
> m    13
> null 1
> rows: 14

SELECT B, CASE B WHEN IS TRUE THEN 1 WHEN IS FALSE THEN 0 WHEN IS UNKNOWN THEN -1 END
    FROM (VALUES TRUE, FALSE, UNKNOWN) T(B);
> B     CASE B WHEN IS TRUE THEN 1 WHEN IS FALSE THEN 0 WHEN IS UNKNOWN THEN -1 END
> ----- ---------------------------------------------------------------------------
> FALSE 0
> TRUE  1
> null  -1
> rows: 3

SELECT J, CASE J WHEN IS JSON ARRAY THEN 1 WHEN IS NOT JSON OBJECT THEN 2 ELSE 3 END
    FROM (VALUES JSON '[]', JSON 'true', JSON '{}') T(J);
> J    CASE J WHEN IS JSON ARRAY THEN 1 WHEN IS NOT JSON OBJECT THEN 2 ELSE 3 END
> ---- --------------------------------------------------------------------------
> []   1
> true 2
> {}   3
> rows: 3

SELECT V, CASE V
    WHEN IN(CURRENT_DATE, DATE '2010-01-01') THEN 1
    ELSE 2
    END FROM (VALUES DATE '2000-01-01', DATE '2010-01-01', DATE '2020-02-01') T(V);
> V          CASE V WHEN IN(CURRENT_DATE, DATE '2010-01-01') THEN 1 ELSE 2 END
> ---------- -----------------------------------------------------------------
> 2000-01-01 2
> 2010-01-01 1
> 2020-02-01 2
> rows: 3

SELECT CASE NULL WHEN IS NOT DISTINCT FROM NULL THEN TRUE ELSE FALSE END;
>> TRUE

SELECT CASE TRUE WHEN CURRENT_DATE THEN 1 END;
> exception TYPES_ARE_NOT_COMPARABLE_2

SELECT * FROM (VALUES 0) D(X) JOIN (VALUES TRUE) T(C) WHERE (CASE C WHEN C THEN C END);
> X C
> - ----
> 0 TRUE
> rows: 1

SELECT CASE TRUE WHEN NOT FALSE THEN 1 ELSE 0 END;
>> 1
