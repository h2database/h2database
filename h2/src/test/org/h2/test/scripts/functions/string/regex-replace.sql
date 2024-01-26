-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

call regexp_replace('x', 'x', '\');
> exception LIKE_ESCAPE_ERROR_1

CALL REGEXP_REPLACE('abckaboooom', 'o+', 'o');
>> abckabom

select regexp_replace('Sylvain', 'S..', 'TOTO', 'mni');
>> TOTOvain

set mode oracle;
> ok

select regexp_replace('.1.2.3.4', '[^0-9]', '', 1, 0);
>> 1234

select regexp_replace('.1.2.3.4', '[^0-9]', '', 1, 1);
>> 1.2.3.4

select regexp_replace('.1.2.3.4', '[^0-9]', '', 1, 2);
>> .12.3.4

select regexp_replace('.1.2.3.4', '[^0-9]', '', 3, 2);
>> .1.23.4

select regexp_replace('', '[^0-9]', '', 3, 2);
>> null

select regexp_replace('ababab', '', '', 3, 2);
>> ababab

select regexp_replace('ababab', '', '', 3, 2, '');
>> ababab

select regexp_replace('first last', '(\w+) (\w+)', '\2 \1');
>> last first

select regexp_replace('first last', '(\w+) (\w+)', '\\2 \1');
>> \2 first

select regexp_replace('first last', '(\w+) (\w+)', '\$2 \1');
>> $2 first

select regexp_replace('first last', '(\w+) (\w+)', '$2 $1');
>> $2 $1

set mode regular;
> ok

select regexp_replace('first last', '(\w+) (\w+)', '\2 \1');
>> 2 1

select regexp_replace('first last', '(\w+) (\w+)', '$2 $1');
>> last first

select regexp_replace('AbcDef', '[^a-z]', '', 'g');
> exception INVALID_VALUE_2

select regexp_replace('First and Second', '[A-Z]', '');
>> irst and econd

set mode PostgreSQL;
> ok

select regexp_replace('AbcDef', '[^a-z]', '', 'g');
>> bcef

select regexp_replace('AbcDef123', '[a-z]', '!', 'gi');
>> !!!!!!123

select regexp_replace('First Only', '[A-Z]', '');
>> irst Only
