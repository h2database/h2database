-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select left(null, 10) en, left('abc', null) en2, left('boat', 2) e_bo, left('', 1) ee, left('a', -1) ee2;
> EN   EN2  E_BO EE EE2
> ---- ---- ---- -- ---
> null null bo
> rows: 1
