-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- case insensitive matches upper case
CALL REGEXP_SUBSTR('A', '[a-z]', 1, 1, 'i');
>> A

-- case sensitive does not match upper case
CALL REGEXP_SUBSTR('A', '[a-z]', 1, 1, 'c');
>> null

-- match string from position at string index 3
CALL REGEXP_SUBSTR('help helpful', 'help.*', 3);
>> helpful

-- match string from position at string index 6
CALL REGEXP_SUBSTR('help helpful helping', 'help.*', 7);
>> helping

-- should return first occurrence
CALL REGEXP_SUBSTR('helpful helping', 'help\w*', 1, 1);
>> helpful

-- should return second occurrence
CALL REGEXP_SUBSTR('helpful helping', 'help\w*', 1, 2);
>> helping

-- should return third occurrence
CALL REGEXP_SUBSTR('help helpful helping', 'help\w*', 1, 3);
>> helping

-- should return first occurrence, after string at index 3
CALL REGEXP_SUBSTR('help helpful helping', 'help\w*', 3, 1);
>> helpful

-- should first matching group
CALL REGEXP_SUBSTR('help helpful helping', '(help\w*)', 1, 1, NULL, 1);
>> help

-- should second occurrence of first group
CALL REGEXP_SUBSTR('help helpful helping', '(help\w*)', 1, 2, NULL, 1);
>> helpful

-- should second group
CALL REGEXP_SUBSTR('2020-10-01', '(\d{4})-(\d{2})-(\d{2})', 1, 1, NULL, 2);
>> 10

-- should third group
CALL REGEXP_SUBSTR('2020-10-01', '(\d{4})-(\d{2})-(\d{2})', 1, 1, NULL, 3);
>> 01

CALL REGEXP_SUBSTR('2020-10-01', '\d{4}');
>> 2020

-- Test variants of passing NULL, which should always result in NULL result
CALL REGEXP_SUBSTR('2020-10-01', NULL);
>> null

CALL REGEXP_SUBSTR(NULL, '\d{4}');
>> null

CALL REGEXP_SUBSTR(NULL, NULL);
>> null

CALL REGEXP_SUBSTR('2020-10-01', '\d{4}', NULL);
>> null

CALL REGEXP_SUBSTR('2020-10-01', '\d{4}', 1, NULL);
>> null

CALL REGEXP_SUBSTR('2020-10-01', '\d{4}', 1, 1, NULL, NULL);
>> null

-- Index out of bounds
CALL REGEXP_SUBSTR('2020-10-01', '(\d{4})', 1, 1, NULL, 10);
>> null

-- Illegal regexp pattern
CALL REGEXP_SUBSTR('2020-10-01', '\d{a}');
> exception LIKE_ESCAPE_ERROR_1

