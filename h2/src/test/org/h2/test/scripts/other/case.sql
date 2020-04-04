-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
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

SELECT CASE END;
>> null

SELECT CASE 1 END;
>> null

SELECT CASE ELSE V END FROM (VALUES 1) T(V);
>> 1

SELECT CASE 1 ELSE V END FROM (VALUES 1) T(V);
>> 1
