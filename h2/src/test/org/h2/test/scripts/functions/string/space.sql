-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select space(null) en, '>' || space(1) || '<' es, '>' || space(3) || '<' e2;
> EN   ES  E2
> ---- --- ---
> null > < > <
> rows: 1
