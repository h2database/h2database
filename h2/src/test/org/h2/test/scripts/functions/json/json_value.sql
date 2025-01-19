-- Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--
--
SELECT JSON_VALUE('[{"type": "a", "a": "t"}, {"type": "b", "a": "u"}]', '$[0,1]?(@.type == "a").a');
>> t

SELECT JSON_VALUE('[{"type": "a", "a": "t"}, {"type": "b", "a": "u"}]', '$[0:1]?(@.type == "a").a');
>> t

SELECT JSON_VALUE('[{"type": "a", "a": "t"}, {"type": "b", "a": "u"}]', '$[0 to 1]?(@.type == "a").a');
>> t

SELECT JSON_VALUE('[{"type": "a", "a": "t"}, {"type": "b", "a": "u"}]', '$[1 - 1 to last]?(@.type == "a").a');
>> t

SELECT JSON_VALUE(
            JSON_ARRAY(10, TRUE, 'str', NULL, '[1,2,3]' FORMAT JSON NULL ON NULL),
            '$[0]' RETURNING BIGINT);
>> 10

SELECT JSON_VALUE(
               JSON_ARRAY(10, TRUE, 'str', NULL, '[1,2,3]' FORMAT JSON NULL ON NULL),
               '$[1]' RETURNING BOOLEAN);
>> TRUE

SELECT JSON_VALUE(
               JSON_ARRAY(10, TRUE, 'str', NULL, '[1,2,3]' FORMAT JSON NULL ON NULL),
               '$[2]' RETURNING VARCHAR(16));
>> str

SELECT JSON_VALUE(
               JSON_ARRAY(10, TRUE, 'str', NULL, '[1,2,3]' FORMAT JSON NULL ON NULL),
               '$[3]' RETURNING BIGINT);
>> null

SELECT JSON_VALUE(
               JSON_ARRAY(10, TRUE, 'str', NULL, '[1,2,3]' FORMAT JSON NULL ON NULL),
               '$[4][2]' RETURNING JSON);
>> 3

SELECT JSON_VALUE(JSON_QUERY(
               JSON_ARRAY(10, TRUE, 'str', NULL, '[1,2,3]' FORMAT JSON NULL ON NULL),
               '$[4]'), '$[1]');
>> 2

SELECT JSON_VALUE('[10, true, "str", null, [1], {"a": 1}]', '$[0]');
>> 10

SELECT JSON_VALUE('[10, true, "str", null, [1], {"a": 1}]', '$[2 * 1 - 1]');
>> TRUE

SELECT JSON_VALUE('[10, true, "str", null, [1], {"a": 1}]', '$[0 + 0]');
>> 10

SELECT JSON_VALUE('[10, true, "str", null, [1], {"a": 1}]', '$[-1 + 1 * 2]');
>> TRUE

SELECT JSON_VALUE('[{"type": "a", "a": "t"}, {"type": "b", "a": "u"}]', '$[*]?(@.type == "a").a');
>> t
