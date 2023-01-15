-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select rtrim(null) en, '>' || rtrim('a') || '<' ea, '>' || rtrim(' a ') || '<' es;
> EN   EA  ES
> ---- --- ----
> null >a< > a<
> rows: 1

select rtrim() from dual;
> exception SYNTAX_ERROR_2

VALUES RTRIM('__A__', '_');
>> __A
