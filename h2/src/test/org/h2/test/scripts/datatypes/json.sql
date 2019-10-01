-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT TYPE_NAME, PRECISION, PREFIX, SUFFIX, PARAMS, MINIMUM_SCALE, MAXIMUM_SCALE FROM INFORMATION_SCHEMA.TYPE_INFO
    WHERE TYPE_NAME = 'JSON';
> TYPE_NAME PRECISION  PREFIX SUFFIX PARAMS MINIMUM_SCALE MAXIMUM_SCALE
> --------- ---------- ------ ------ ------ ------------- -------------
> JSON      2147483647 JSON ' '      LENGTH 0             0
> rows: 1

SELECT '{"tag1":"simple string"}' FORMAT JSON;
>> {"tag1":"simple string"}

SELECT CAST('{"tag1":"simple string"}' FORMAT JSON AS JSON);
>> {"tag1":"simple string"}

SELECT CAST('text' AS JSON);
>> "text"

SELECT X'31' FORMAT JSON;
>> 1

SELECT 0::JSON;
>> 0

SELECT '0' FORMAT JSON;
>> 0

SELECT JSON '1', JSON X'31', JSON '1' IS OF (JSON), JSON X'31' IS OF (JSON);
> JSON '1' JSON '1' TRUE TRUE
> -------- -------- ---- ----
> 1        1        TRUE TRUE
> rows: 1

SELECT JSON 'tr' 'ue', JSON X'7472' '7565', JSON 'tr' 'ue' IS OF (JSON), JSON X'7472' '7565' IS OF (JSON);
> JSON 'true' JSON 'true' TRUE TRUE
> ----------- ----------- ---- ----
> true        true        TRUE TRUE
> rows: 1

SELECT 1::JSON;
>> 1

SELECT 1L::JSON;
>> 1

SELECT 1000000000000L::JSON;
>> 1000000000000

SELECT CAST(1e100::FLOAT AS JSON);
>> 1.0E100

SELECT CAST(1e100::DOUBLE AS JSON);
>> 1.0E100

SELECT CAST(1e100::NUMERIC AS JSON);
>> 1E100

SELECT CAST(TRUE AS JSON);
>> true

SELECT CAST('true' FORMAT JSON AS JSON);
>> true

SELECT CAST(FALSE AS JSON);
>> false

SELECT CAST('false' FORMAT JSON AS JSON);
>> false

SELECT CAST('null' FORMAT JSON AS JSON);
>> null

SELECT CAST('10' FORMAT JSON AS VARBINARY);
>> 3130

SELECT CAST('10' FORMAT JSON AS BLOB);
>> 3130

CREATE TABLE TEST (ID INT, DATA JSON);
> ok

INSERT INTO TEST VALUES
(1, '{"tag1":"simple string", "tag2": 333, "tag3":[1, 2, 3]}' format json),
(2, '{"tag1":"another string", "tag4":{"lvl1":"lvl2"}}' format json),
(3, '["string", 5555, {"arr":"yes"}]' format json),
(4, '{"1":"val1"}' format json);
> update count: 4

@reconnect

SELECT ID, DATA FROM TEST;
> ID DATA
> -- --------------------------------------------------
> 1  {"tag1":"simple string","tag2":333,"tag3":[1,2,3]}
> 2  {"tag1":"another string","tag4":{"lvl1":"lvl2"}}
> 3  ["string",5555,{"arr":"yes"}]
> 4  {"1":"val1"}
> rows: 4

INSERT INTO TEST VALUES (5, '}' FORMAT JSON);
> exception DATA_CONVERSION_ERROR_1

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID INT, S VARCHAR, B VARBINARY, J JSON) AS VALUES
    (1, '{"a":1,"a":2}', STRINGTOUTF8('{"a":1,"a":2}'), '{"a":1,"a":2}' FORMAT JSON),
    (2, '{"a":1,"b":2}', STRINGTOUTF8('{"a":1,"b":2}'), '{"a":1,"b":2}' FORMAT JSON),
    (3, '{"a":1,"b":2', STRINGTOUTF8('{"a":1,"b":2'), null),
    (4, null, null, null);
> ok

SELECT S IS JSON, B IS JSON WITHOUT UNIQUE, J IS JSON WITHOUT UNIQUE KEYS FROM TEST ORDER BY ID;
> S IS JSON B IS JSON J IS JSON
> --------- --------- ---------
> TRUE      TRUE      TRUE
> TRUE      TRUE      TRUE
> FALSE     FALSE     null
> null      null      null
> rows (ordered): 4

SELECT S IS NOT JSON, B IS NOT JSON WITHOUT UNIQUE, J IS NOT JSON WITHOUT UNIQUE KEYS FROM TEST ORDER BY ID;
> S IS NOT JSON B IS NOT JSON J IS NOT JSON
> ------------- ------------- -------------
> FALSE         FALSE         FALSE
> FALSE         FALSE         FALSE
> TRUE          TRUE          null
> null          null          null
> rows (ordered): 4

SELECT S IS JSON WITH UNIQUE KEYS, B IS JSON WITH UNIQUE, J IS JSON WITH UNIQUE KEYS FROM TEST ORDER BY ID;
> S IS JSON WITH UNIQUE KEYS B IS JSON WITH UNIQUE KEYS J IS JSON WITH UNIQUE KEYS
> -------------------------- -------------------------- --------------------------
> FALSE                      FALSE                      FALSE
> TRUE                       TRUE                       TRUE
> FALSE                      FALSE                      null
> null                       null                       null
> rows (ordered): 4

SELECT S IS NOT JSON WITH UNIQUE KEYS, B IS NOT JSON WITH UNIQUE, J IS NOT JSON WITH UNIQUE KEYS FROM TEST ORDER BY ID;
> S IS NOT JSON WITH UNIQUE KEYS B IS NOT JSON WITH UNIQUE KEYS J IS NOT JSON WITH UNIQUE KEYS
> ------------------------------ ------------------------------ ------------------------------
> TRUE                           TRUE                           TRUE
> FALSE                          FALSE                          FALSE
> TRUE                           TRUE                           null
> null                           null                           null
> rows (ordered): 4

DROP TABLE TEST;
> ok

SELECT 1 IS JSON;
>> FALSE

SELECT 1 IS NOT JSON;
>> TRUE

CREATE TABLE TEST(ID INT, S VARCHAR) AS VALUES
    (1, '[{"a":1}]'), (2, '{"a":[3]}'),
    (3, 'null'), (4, '{"a":1,"a":2}'),
    (5, 'X'), (6, NULL);
> ok

EXPLAIN SELECT S FORMAT JSON FORMAT JSON, (S FORMAT JSON) FORMAT JSON FROM TEST;
>> SELECT "S" FORMAT JSON, "S" FORMAT JSON FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

ALTER TABLE TEST ADD J JSON;
> ok

UPDATE TEST SET J = S FORMAT JSON WHERE S IS JSON;
> update count: 4

SELECT S IS JSON, S IS JSON VALUE, S IS JSON ARRAY, S IS JSON OBJECT, S IS JSON SCALAR FROM TEST ORDER BY ID;
> S IS JSON S IS JSON S IS JSON ARRAY S IS JSON OBJECT S IS JSON SCALAR
> --------- --------- --------------- ---------------- ----------------
> TRUE      TRUE      TRUE            FALSE            FALSE
> TRUE      TRUE      FALSE           TRUE             FALSE
> TRUE      TRUE      FALSE           FALSE            TRUE
> TRUE      TRUE      FALSE           TRUE             FALSE
> FALSE     FALSE     FALSE           FALSE            FALSE
> null      null      null            null             null
> rows (ordered): 6

SELECT J IS JSON, J IS JSON VALUE, J IS JSON ARRAY, J IS JSON OBJECT, J IS JSON SCALAR FROM TEST ORDER BY ID;
> J IS JSON J IS JSON J IS JSON ARRAY J IS JSON OBJECT J IS JSON SCALAR
> --------- --------- --------------- ---------------- ----------------
> TRUE      TRUE      TRUE            FALSE            FALSE
> TRUE      TRUE      FALSE           TRUE             FALSE
> TRUE      TRUE      FALSE           FALSE            TRUE
> TRUE      TRUE      FALSE           TRUE             FALSE
> null      null      null            null             null
> null      null      null            null             null
> rows (ordered): 6

SELECT J IS JSON WITH UNIQUE KEYS, J IS JSON VALUE WITH UNIQUE KEYS, J IS JSON ARRAY WITH UNIQUE KEYS,
    J IS JSON OBJECT WITH UNIQUE KEYS, J IS JSON SCALAR WITH UNIQUE KEYS FROM TEST ORDER BY ID;
> J IS JSON WITH UNIQUE KEYS J IS JSON WITH UNIQUE KEYS J IS JSON ARRAY WITH UNIQUE KEYS J IS JSON OBJECT WITH UNIQUE KEYS J IS JSON SCALAR WITH UNIQUE KEYS
> -------------------------- -------------------------- -------------------------------- --------------------------------- ---------------------------------
> TRUE                       TRUE                       TRUE                             FALSE                             FALSE
> TRUE                       TRUE                       FALSE                            TRUE                              FALSE
> TRUE                       TRUE                       FALSE                            FALSE                             TRUE
> FALSE                      FALSE                      FALSE                            FALSE                             FALSE
> null                       null                       null                             null                              null
> null                       null                       null                             null                              null
> rows (ordered): 6

SELECT S IS NOT JSON, S IS NOT JSON VALUE, S IS NOT JSON ARRAY, S IS NOT JSON OBJECT, S IS NOT JSON SCALAR
    FROM TEST ORDER BY ID;
> S IS NOT JSON S IS NOT JSON S IS NOT JSON ARRAY S IS NOT JSON OBJECT S IS NOT JSON SCALAR
> ------------- ------------- ------------------- -------------------- --------------------
> FALSE         FALSE         FALSE               TRUE                 TRUE
> FALSE         FALSE         TRUE                FALSE                TRUE
> FALSE         FALSE         TRUE                TRUE                 FALSE
> FALSE         FALSE         TRUE                FALSE                TRUE
> TRUE          TRUE          TRUE                TRUE                 TRUE
> null          null          null                null                 null
> rows (ordered): 6

SELECT NOT S IS NOT JSON, NOT S IS NOT JSON VALUE, NOT S IS NOT JSON ARRAY, NOT S IS NOT JSON OBJECT,
    NOT S IS NOT JSON SCALAR FROM TEST ORDER BY ID;
> S IS JSON S IS JSON S IS JSON ARRAY S IS JSON OBJECT S IS JSON SCALAR
> --------- --------- --------------- ---------------- ----------------
> TRUE      TRUE      TRUE            FALSE            FALSE
> TRUE      TRUE      FALSE           TRUE             FALSE
> TRUE      TRUE      FALSE           FALSE            TRUE
> TRUE      TRUE      FALSE           TRUE             FALSE
> FALSE     FALSE     FALSE           FALSE            FALSE
> null      null      null            null             null
> rows (ordered): 6

DROP TABLE TEST;
> ok

SELECT NULL FORMAT JSON, (NULL FORMAT JSON) IS NULL;
> JSON 'null' FALSE
> ----------- -----
> null        FALSE
> rows: 1

CREATE MEMORY TABLE TEST(J JSON) AS VALUES ('["\u00A7''",{}]' FORMAT JSON);
> ok

SCRIPT NOPASSWORDS NOSETTINGS TABLE TEST;
> SCRIPT
> ----------------------------------------------------------------
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "J" JSON );
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> INSERT INTO "PUBLIC"."TEST" VALUES (JSON '["\u00a7\u0027",{}]');
> rows: 4

DROP TABLE TEST;
> ok
