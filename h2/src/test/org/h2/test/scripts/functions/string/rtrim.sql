-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select rtrim(null) en, '>' || rtrim('a') || '<' ea, '>' || rtrim(' a ') || '<' es;
> EN   EA  ES
> ---- --- ----
> null >a< > a<
> rows: 1

select rtrim() from dual;
> exception INVALID_PARAMETER_COUNT_2
