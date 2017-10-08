----- Issue#600 -----
create table test as (select char(x) as str from system_range(48,90));
> ok

select row_number() over () as rnum, str from test where str = 'A';
> RNUM STR
> ---- ---
> 1    A

drop table test;
> ok

