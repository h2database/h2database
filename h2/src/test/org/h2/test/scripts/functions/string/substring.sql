-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
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

-- Compatibility syntax
select substr('Hello World', -5);
>> World

-- Compatibility
SELECT SUBSTRING('X', 0, 1);
>> X

CREATE TABLE TEST(STR VARCHAR, START INT, LEN INT);
> ok

EXPLAIN SELECT SUBSTRING(STR FROM START), SUBSTRING(STR FROM START FOR LEN) FROM TEST;
>> SELECT SUBSTRING("STR" FROM "START"), SUBSTRING("STR" FROM "START" FOR "LEN") FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

DROP TABLE TEST;
> ok

SELECT SUBSTRING('AAA' FROM 4 FOR 1);
> ''
> --
>
> rows: 1

SELECT SUBSTRING(X'001122' FROM 1 FOR 3);
>> X'001122'

SELECT SUBSTRING(X'001122' FROM 1 FOR 2);
>> X'0011'

SELECT SUBSTRING(X'001122' FROM 2 FOR 2);
>> X'1122'

SELECT SUBSTRING(X'001122' FROM 4 FOR 1);
>> X''

SELECT SUBSTRING(X'001122' FROM 2 FOR 1);
>> X'11'

CREATE MEMORY TABLE TEST AS (VALUES SUBSTRING(X'0011' FROM 2));
> ok

-- Compatibility
SELECT SUBSTRING(X'00', 0, 1);
>> X'00'

SCRIPT NOPASSWORDS NOSETTINGS NOVERSION TABLE TEST;
> SCRIPT
> --------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "C1" BINARY VARYING(1) );
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> INSERT INTO "PUBLIC"."TEST" VALUES (X'11');
> rows (ordered): 4

DROP TABLE TEST;
> ok
