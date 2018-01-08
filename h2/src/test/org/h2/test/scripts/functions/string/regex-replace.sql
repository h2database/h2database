-- Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

call regexp_replace('x', 'x', '\');
> exception

CALL REGEXP_REPLACE('abckaboooom', 'o+', 'o');
> 'abckabom'
> ----------
> abckabom
> rows: 1

select regexp_replace('Sylvain', 'S..', 'TOTO', 'mni') as X;
> X
> --------
> TOTOvain
> rows: 1

set mode oracle;

select regexp_replace('first last', '(\w+) (\w+)', '\2 \1') as X from dual;
> X
> ----------
> last first
> rows: 1

select regexp_replace('first last', '(\w+) (\w+)', '\\2 \1') as X from dual;
> X
> --------
> \2 first
> rows: 1

select regexp_replace('first last', '(\w+) (\w+)', '\$2 \1') as X from dual;
> X
> --------
> $2 first
> rows: 1

select regexp_replace('first last', '(\w+) (\w+)', '$2 $1') as X from dual;
> X
> -----
> $2 $1
> rows: 1

set mode regular;

select regexp_replace('first last', '(\w+) (\w+)', '\2 \1') as X from dual;
> X
> ---
> 2 1
> rows: 1

select regexp_replace('first last', '(\w+) (\w+)', '$2 $1') as X from dual;
> X
> ----------
> last first
> rows: 1
