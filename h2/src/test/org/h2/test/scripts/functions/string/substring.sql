-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select substr(null, null) en, substr(null, null, null) e1, substr('bob', 2) e_ob, substr('bob', 2, 1) eo;
> EN   E1   E_OB EO
> ---- ---- ---- --
> null null ob   o
> rows: 1

select substring(null, null) en, substring(null, null, null) e1, substring('bob', 2) e_ob, substring('bob', 2, 1) eo;
> EN   E1   E_OB EO
> ---- ---- ---- --
> null null ob   o
> rows: 1

select substring(null from null) en, substring(null from null for null) e1, substring('bob' from 2) e_ob, substring('bob' from 2 for 1) eo;
> EN   E1   E_OB EO
> ---- ---- ---- --
> null null ob   o
> rows: 1

select substr('[Hello]', 2, 5);
>> Hello

select substr('Hello World', -5);
>> World

CREATE TABLE TEST(STR VARCHAR, START INT, LEN INT);
> ok

EXPLAIN SELECT SUBSTRING(STR FROM START), SUBSTRING(STR FROM START FOR LEN) FROM TEST;
>> SELECT SUBSTRING("STR" FROM "START"), SUBSTRING("STR" FROM "START" FOR "LEN") FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

DROP TABLE TEST;
> ok
