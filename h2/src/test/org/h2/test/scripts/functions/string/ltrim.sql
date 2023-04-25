-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select ltrim(null) en, '>' || ltrim('a') || '<' ea, '>' || ltrim(' a ') || '<' e_as;
> EN   EA  E_AS
> ---- --- ----
> null >a< >a <
> rows: 1

VALUES LTRIM('__A__', '_');
>> A__
