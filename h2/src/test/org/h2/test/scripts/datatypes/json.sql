-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT CAST('{"tag1":"simple string"}' AS JSON);
>> {"tag1":"simple string"}

SELECT '{"tag1":"simple string"}'::JSON;
>> {"tag1":"simple string"}

SELECT X'31'::JSON;
>> 1

SELECT 0::JSON;
>> 0

SELECT '0'::JSON;
>> 0

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

SELECT CAST('true' AS JSON);
>> true

SELECT CAST(FALSE AS JSON);
>> false

SELECT CAST('false' AS JSON);
>> false

SELECT CAST('null' AS JSON);
>> null

SELECT CAST('10'::JSON AS VARBINARY);
>> 3130

SELECT CAST('10'::JSON AS BLOB);
>> 3130

CREATE TABLE TEST (ID INT, DATA JSON);
> ok

INSERT INTO TEST VALUES
(1, '{"tag1":"simple string", "tag2": 333, "tag3":[1, 2, 3]}'::json),
(2, '{"tag1":"another string", "tag4":{"lvl1":"lvl2"}}'::json),
(3, '["string", 5555, {"arr":"yes"}]'::json),
(4, '{"1":"val1"}'::json);
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

INSERT INTO TEST VALUES (5, '}');
> exception DATA_CONVERSION_ERROR_1

DROP TABLE TEST;
> ok
