-- Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

EXPLAIN SELECT JSON_EXISTS(J, 'lax $.name') FROM (VALUES JSON '{"name": "Name"}') T(J);
>> SELECT JSON_EXISTS("J", 'lax $.name') FROM (VALUES (JSON '{"name":"Name"}')) "T"("J") /* table scan */

EXPLAIN SELECT
    JSON_EXISTS(J, 'lax $.name' PASSING JSON '1' AS A),
    JSON_EXISTS(J, 'lax $.name' PASSING JSON '1' AS A, JSON '2' AS B)
    FROM (VALUES JSON '{"name": "Name"}') T(J);
>> SELECT JSON_EXISTS("J", 'lax $.name' PASSING JSON '1' AS "A"), JSON_EXISTS("J", 'lax $.name' PASSING JSON '1' AS "A", JSON '2' AS "B") FROM (VALUES (JSON '{"name":"Name"}')) "T"("J") /* table scan */

EXPLAIN SELECT JSON_EXISTS(J, 'lax $.name' TRUE ON ERROR), JSON_EXISTS(J, 'lax $.name' FALSE ON ERROR),
    JSON_EXISTS(J, 'lax $.name' UNKNOWN ON ERROR), JSON_EXISTS(J, 'lax $.name' ERROR ON ERROR)
    FROM (VALUES JSON '{"name": "Name"}') T(J);
>> SELECT JSON_EXISTS("J", 'lax $.name' TRUE ON ERROR), JSON_EXISTS("J", 'lax $.name'), JSON_EXISTS("J", 'lax $.name' UNKNOWN ON ERROR), JSON_EXISTS("J", 'lax $.name' ERROR ON ERROR) FROM (VALUES (JSON '{"name":"Name"}')) "T"("J") /* table scan */

SELECT JSON_EXISTS(J, 'lax $.name' PASSING JSON '1' AS A, JSON '2' AS A)
    FROM (VALUES JSON '{"name": "Name"}') T(J);
> exception DUPLICATE_COLUMN_NAME_1

SELECT JSON_EXISTS(NULL, '$.a');
>> null

SELECT JSON_EXISTS(JSON '{"a" : 1}', '$.a');
>> TRUE

SELECT JSON_EXISTS(JSON '{"a" : 1}', '$.b');
>> FALSE

SELECT JSON_EXISTS(JSON '{"a" : 1}', 'strict $.b');
>> FALSE

SELECT JSON_EXISTS(JSON '{"a" : 1}', 'strict $.b' TRUE ON ERROR);
>> TRUE

SELECT JSON_EXISTS(JSON '{"a" : 1}', 'strict $.b' FALSE ON ERROR);
>> FALSE

SELECT JSON_EXISTS(JSON '{"a" : 1}', 'strict $.b' UNKNOWN ON ERROR);
>> null

SELECT JSON_EXISTS(JSON '{"a" : 1}', 'strict $.b' ERROR ON ERROR);
> exception INVALID_VALUE_2
