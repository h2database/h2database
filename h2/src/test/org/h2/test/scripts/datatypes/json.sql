-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT CAST('{"tag1":"simple string"}' AS JSON);
>> {"tag1":"simple string"}

SELECT '{"tag1":"simple string"}'::JSON;
>> {"tag1":"simple string"}

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
>> <4 rows>

DROP TABLE TEST IF EXISTS;
> ok
