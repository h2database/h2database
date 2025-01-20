-- Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

EXPLAIN SELECT JSON_QUERY(J, 'lax $.name'), JSON_QUERY(J, 'lax $.name' RETURNING BINARY VARYING)
    FROM (VALUES JSON '{"name": "Name"}') T(J);
>> SELECT JSON_QUERY("J", 'lax $.name' RETURNING JSON), JSON_QUERY("J", 'lax $.name' RETURNING BINARY VARYING FORMAT JSON) FROM (VALUES (JSON '{"name":"Name"}')) "T"("J") /* table scan */

EXPLAIN SELECT
    JSON_QUERY(J, 'lax $.name' RETURNING BINARY VARYING FORMAT JSON),
    JSON_QUERY(J, 'lax $.name' RETURNING BINARY VARYING FORMAT JSON ENCODING UTF8)
    FROM (VALUES JSON '{"name": "Name"}') T(J);
>> SELECT JSON_QUERY("J", 'lax $.name' RETURNING BINARY VARYING FORMAT JSON), JSON_QUERY("J", 'lax $.name' RETURNING BINARY VARYING FORMAT JSON ENCODING UTF8) FROM (VALUES (JSON '{"name":"Name"}')) "T"("J") /* table scan */

EXPLAIN SELECT
    JSON_QUERY(J, 'lax $.name' RETURNING BINARY VARYING FORMAT JSON ENCODING UTF16),
    JSON_QUERY(J, 'lax $.name' RETURNING BINARY VARYING FORMAT JSON ENCODING UTF32)
    FROM (VALUES JSON '{"name": "Name"}') T(J);
>> SELECT JSON_QUERY("J", 'lax $.name' RETURNING BINARY VARYING FORMAT JSON ENCODING UTF16BE), JSON_QUERY("J", 'lax $.name' RETURNING BINARY VARYING FORMAT JSON ENCODING UTF32BE) FROM (VALUES (JSON '{"name":"Name"}')) "T"("J") /* table scan */

EXPLAIN SELECT
    JSON_QUERY(J, 'lax $.name' RETURNING BINARY VARYING FORMAT JSON ENCODING UTF16BE),
    JSON_QUERY(J, 'lax $.name' RETURNING BINARY VARYING FORMAT JSON ENCODING UTF16LE)
    FROM (VALUES JSON '{"name": "Name"}') T(J);
>> SELECT JSON_QUERY("J", 'lax $.name' RETURNING BINARY VARYING FORMAT JSON ENCODING UTF16BE), JSON_QUERY("J", 'lax $.name' RETURNING BINARY VARYING FORMAT JSON ENCODING UTF16LE) FROM (VALUES (JSON '{"name":"Name"}')) "T"("J") /* table scan */

EXPLAIN SELECT
    JSON_QUERY(J, 'lax $.name' RETURNING BINARY VARYING FORMAT JSON ENCODING UTF32BE),
    JSON_QUERY(J, 'lax $.name' RETURNING BINARY VARYING FORMAT JSON ENCODING UTF32LE)
    FROM (VALUES JSON '{"name": "Name"}') T(J);
>> SELECT JSON_QUERY("J", 'lax $.name' RETURNING BINARY VARYING FORMAT JSON ENCODING UTF32BE), JSON_QUERY("J", 'lax $.name' RETURNING BINARY VARYING FORMAT JSON ENCODING UTF32LE) FROM (VALUES (JSON '{"name":"Name"}')) "T"("J") /* table scan */

EXPLAIN SELECT
    JSON_QUERY(J, 'lax $.name' WITHOUT WRAPPER),
    JSON_QUERY(J, 'lax $.name' WITHOUT ARRAY WRAPPER)
    FROM (VALUES JSON '{"name": "Name"}') T(J);
>> SELECT JSON_QUERY("J", 'lax $.name' RETURNING JSON), JSON_QUERY("J", 'lax $.name' RETURNING JSON) FROM (VALUES (JSON '{"name":"Name"}')) "T"("J") /* table scan */

EXPLAIN SELECT
    JSON_QUERY(J, 'lax $.name' WITH WRAPPER),
    JSON_QUERY(J, 'lax $.name' WITH ARRAY WRAPPER)
    FROM (VALUES JSON '{"name": "Name"}') T(J);
>> SELECT JSON_QUERY("J", 'lax $.name' RETURNING JSON WITH UNCONDITIONAL ARRAY WRAPPER), JSON_QUERY("J", 'lax $.name' RETURNING JSON WITH UNCONDITIONAL ARRAY WRAPPER) FROM (VALUES (JSON '{"name":"Name"}')) "T"("J") /* table scan */

EXPLAIN SELECT
    JSON_QUERY(J, 'lax $.name' WITH CONDITIONAL WRAPPER),
    JSON_QUERY(J, 'lax $.name' WITH CONDITIONAL ARRAY WRAPPER)
    FROM (VALUES JSON '{"name": "Name"}') T(J);
>> SELECT JSON_QUERY("J", 'lax $.name' RETURNING JSON WITH CONDITIONAL ARRAY WRAPPER), JSON_QUERY("J", 'lax $.name' RETURNING JSON WITH CONDITIONAL ARRAY WRAPPER) FROM (VALUES (JSON '{"name":"Name"}')) "T"("J") /* table scan */

EXPLAIN SELECT
    JSON_QUERY(J, 'lax $.name' WITH UNCONDITIONAL WRAPPER),
    JSON_QUERY(J, 'lax $.name' WITH UNCONDITIONAL ARRAY WRAPPER)
    FROM (VALUES JSON '{"name": "Name"}') T(J);
>> SELECT JSON_QUERY("J", 'lax $.name' RETURNING JSON WITH UNCONDITIONAL ARRAY WRAPPER), JSON_QUERY("J", 'lax $.name' RETURNING JSON WITH UNCONDITIONAL ARRAY WRAPPER) FROM (VALUES (JSON '{"name":"Name"}')) "T"("J") /* table scan */

SELECT JSON_QUERY(J, 'lax $.name' WITH ARRAY WRAPPER OMIT QUOTES);
> exception SYNTAX_ERROR_2

EXPLAIN SELECT
    JSON_QUERY(J, 'lax $.name' KEEP QUOTES),
    JSON_QUERY(J, 'lax $.name' KEEP QUOTES ON SCALAR STRING)
    FROM (VALUES JSON '{"name": "Name"}') T(J);
>> SELECT JSON_QUERY("J", 'lax $.name' RETURNING JSON), JSON_QUERY("J", 'lax $.name' RETURNING JSON) FROM (VALUES (JSON '{"name":"Name"}')) "T"("J") /* table scan */

EXPLAIN SELECT
    JSON_QUERY(J, 'lax $.name' OMIT QUOTES),
    JSON_QUERY(J, 'lax $.name' OMIT QUOTES ON SCALAR STRING)
    FROM (VALUES JSON '{"name": "Name"}') T(J);
>> SELECT JSON_QUERY("J", 'lax $.name' RETURNING JSON OMIT QUOTES), JSON_QUERY("J", 'lax $.name' RETURNING JSON OMIT QUOTES) FROM (VALUES (JSON '{"name":"Name"}')) "T"("J") /* table scan */

EXPLAIN SELECT
    JSON_QUERY(J, 'lax $.name' ERROR ON EMPTY),
    JSON_QUERY(J, 'lax $.name' NULL ON EMPTY),
    JSON_QUERY(J, 'lax $.name' EMPTY ARRAY ON EMPTY),
    JSON_QUERY(J, 'lax $.name' EMPTY OBJECT ON EMPTY)
    FROM (VALUES JSON '{"name": "Name"}') T(J);
>> SELECT JSON_QUERY("J", 'lax $.name' RETURNING JSON ERROR ON EMPTY), JSON_QUERY("J", 'lax $.name' RETURNING JSON), JSON_QUERY("J", 'lax $.name' RETURNING JSON EMPTY ARRAY ON EMPTY), JSON_QUERY("J", 'lax $.name' RETURNING JSON EMPTY OBJECT ON EMPTY) FROM (VALUES (JSON '{"name":"Name"}')) "T"("J") /* table scan */

EXPLAIN SELECT
    JSON_QUERY(J, 'lax $.name' ERROR ON ERROR),
    JSON_QUERY(J, 'lax $.name' NULL ON ERROR),
    JSON_QUERY(J, 'lax $.name' EMPTY ARRAY ON ERROR),
    JSON_QUERY(J, 'lax $.name' EMPTY OBJECT ON ERROR)
    FROM (VALUES JSON '{"name": "Name"}') T(J);
>> SELECT JSON_QUERY("J", 'lax $.name' RETURNING JSON ERROR ON ERROR), JSON_QUERY("J", 'lax $.name' RETURNING JSON), JSON_QUERY("J", 'lax $.name' RETURNING JSON EMPTY ARRAY ON ERROR), JSON_QUERY("J", 'lax $.name' RETURNING JSON EMPTY OBJECT ON ERROR) FROM (VALUES (JSON '{"name":"Name"}')) "T"("J") /* table scan */

EXPLAIN SELECT
    JSON_QUERY(J, 'lax $.name' ERROR ON EMPTY ERROR ON ERROR),
    JSON_QUERY(J, 'lax $.name' ERROR ON EMPTY NULL ON ERROR),
    JSON_QUERY(J, 'lax $.name' ERROR ON EMPTY EMPTY ARRAY ON ERROR),
    JSON_QUERY(J, 'lax $.name' ERROR ON EMPTY EMPTY OBJECT ON ERROR)
    FROM (VALUES JSON '{"name": "Name"}') T(J);
>> SELECT JSON_QUERY("J", 'lax $.name' RETURNING JSON ERROR ON EMPTY ERROR ON ERROR), JSON_QUERY("J", 'lax $.name' RETURNING JSON ERROR ON EMPTY), JSON_QUERY("J", 'lax $.name' RETURNING JSON ERROR ON EMPTY EMPTY ARRAY ON ERROR), JSON_QUERY("J", 'lax $.name' RETURNING JSON ERROR ON EMPTY EMPTY OBJECT ON ERROR) FROM (VALUES (JSON '{"name":"Name"}')) "T"("J") /* table scan */

SELECT JSON_QUERY(JSON '[]', '$[*]' ERROR ON EMPTY);
> exception INVALID_VALUE_2

SELECT JSON_QUERY(JSON '[]', '$[*]' NULL ON EMPTY);
>> null

SELECT JSON_QUERY(JSON '[]', '$[*]' EMPTY ARRAY ON EMPTY);
>> []

SELECT JSON_QUERY(JSON '[]', '$[*]' EMPTY OBJECT ON EMPTY);
>> {}

SELECT JSON_QUERY(JSON '[]', 'strict $[0]' ERROR ON ERROR);
> exception INVALID_VALUE_2

SELECT JSON_QUERY(JSON '[]', 'strict $[0]' NULL ON ERROR);
>> null

SELECT JSON_QUERY(JSON '[]', 'strict $[0]' EMPTY ARRAY ON ERROR);
>> []

SELECT JSON_QUERY(JSON '[]', 'strict $[0]' EMPTY OBJECT ON ERROR);
>> {}

SELECT JSON_QUERY(JSON '"1"', '$' RETURNING VARCHAR);
>> "1"

SELECT JSON_QUERY(JSON '"1"', '$' RETURNING VARCHAR) IS OF (VARCHAR);
>> TRUE

SELECT FORMAT, BYTES FROM (VALUES
    (1, '', JSON_QUERY(JSON '"1"', '$' RETURNING VARBINARY)),
    (2, 'FORMAT JSON', JSON_QUERY(JSON '"1"', '$' RETURNING VARBINARY FORMAT JSON)),
    (3, 'FORMAT JSON ENCODING UTF8', JSON_QUERY(JSON '"1"', '$' RETURNING VARBINARY FORMAT JSON ENCODING UTF8)),
    (4, 'FORMAT JSON ENCODING UTF16', JSON_QUERY(JSON '"1"', '$' RETURNING VARBINARY FORMAT JSON ENCODING UTF16)),
    (5, 'FORMAT JSON ENCODING UTF16BE', JSON_QUERY(JSON '"1"', '$' RETURNING VARBINARY FORMAT JSON ENCODING UTF16BE)),
    (6, 'FORMAT JSON ENCODING UTF16LE', JSON_QUERY(JSON '"1"', '$' RETURNING VARBINARY FORMAT JSON ENCODING UTF16LE)),
    (7, 'FORMAT JSON ENCODING UTF32', JSON_QUERY(JSON '"1"', '$' RETURNING VARBINARY FORMAT JSON ENCODING UTF32)),
    (8, 'FORMAT JSON ENCODING UTF32BE', JSON_QUERY(JSON '"1"', '$' RETURNING VARBINARY FORMAT JSON ENCODING UTF32BE)),
    (9, 'FORMAT JSON ENCODING UTF32LE', JSON_QUERY(JSON '"1"', '$' RETURNING VARBINARY FORMAT JSON ENCODING UTF32LE)))
T(ID, FORMAT, BYTES) ORDER BY ID;
> FORMAT                       BYTES
> ---------------------------- ---------------------------
>                              X'223122'
> FORMAT JSON                  X'223122'
> FORMAT JSON ENCODING UTF8    X'223122'
> FORMAT JSON ENCODING UTF16   X'002200310022'
> FORMAT JSON ENCODING UTF16BE X'002200310022'
> FORMAT JSON ENCODING UTF16LE X'220031002200'
> FORMAT JSON ENCODING UTF32   X'000000220000003100000022'
> FORMAT JSON ENCODING UTF32BE X'000000220000003100000022'
> FORMAT JSON ENCODING UTF32LE X'220000003100000022000000'
> rows (ordered): 9

SELECT JSON_QUERY(NULL, '1');
>> null

SELECT JSON_QUERY(JSON '11', '$');
>> 11

SELECT JSON_QUERY(JSON '[10, 20, 30]', '$[last]' WITH UNCONDITIONAL ARRAY WRAPPER);
>> [30]

SELECT JSON_QUERY(JSON '[10, 20, 30, 40, 50, 60, 70]', '$[1 to 2, 4, last - 1]' WITH ARRAY WRAPPER);
>> [20,30,50,60]

SELECT JSON_QUERY(JSON '[10, 20, 30]', '$[*]' WITHOUT ARRAY WRAPPER);
>> null

SELECT JSON_QUERY(JSON '[10, 20, 30]', '$[*]' WITH ARRAY WRAPPER);
>> [10,20,30]

SELECT JSON_QUERY(JSON '1', 'strict $[0]');
>> null

SELECT JSON_QUERY(JSON '1', 'lax $[0]');
>> 1

SELECT JSON_QUERY(JSON '{"a": "b", "c": "d"}', '$.a');
>> "b"

SELECT JSON_QUERY(JSON '{"a": "b", "c": "d"}', '$."a"');
>> "b"

SELECT JSON_QUERY(JSON '{"a": "b", "c": "d"}', '$.*' WITH ARRAY WRAPPER);
>> ["b","d"]

SELECT JSON_QUERY(JSON 'null', '-0');
>> 0

SELECT JSON_QUERY(JSON '"text"', '-$' ERROR ON ERROR);
> exception INVALID_VALUE_2

SELECT JSON_QUERY(JSON 'null', '((1 + 2 * 3 - -+4) / 2).floor() % 3');
>> 2

SELECT JSON_QUERY(JSON '1', '"abc"');
>> "abc"

SELECT JSON_QUERY(JSON '1', 'null');
>> null

SELECT JSON_QUERY(JSON '1', 'true');
>> true

SELECT JSON_QUERY(JSON '1', 'false');
>> false

SELECT JSON_QUERY(JSON '1', '$P1' PASSING JSON '"text"' AS P1);
>> "text"

SELECT JSON_QUERY(JSON 'null', '$.type()');
>> "null"

SELECT JSON_QUERY(JSON 'true', '$.type()');
>> "boolean"

SELECT JSON_QUERY(JSON '1', '$.type()');
>> "number"

SELECT JSON_QUERY(JSON '"aaa"', '$.type()');
>> "string"

SELECT JSON_QUERY(JSON '"2024-12-31"', '$.datetime("YYYY-MM-DD").type()');
>> "date"

SELECT JSON_QUERY(JSON '"10:11:33"', '$.datetime("HH24:MI:SS").type()');
>> "time without time zone"

SELECT JSON_QUERY(JSON '"10:11:33 +10:00"', '$.datetime("HH24:MI:SS TZH:TZM").type()');
>> "time with time zone"

SELECT JSON_QUERY(JSON '"2024-12-31 10:11:33"', '$.datetime("YYYY-MM-DD HH24:MI:SS").type()');
>> "timestamp without time zone"

SELECT JSON_QUERY(JSON '"2024-12-31 10:11:33 +10:00"', '$.datetime("YYYY-MM-DD HH24:MI:SS TZH:TZM").type()');
>> "timestamp with time zone"

SELECT JSON_QUERY(JSON '[]', '$.type()');
>> "array"

SELECT JSON_QUERY(JSON '{}', '$.type()');
>> "object"

SELECT JSON_QUERY(JSON '[1, 2]', '$.size()');
>> 2

SELECT JSON_QUERY(JSON '4', '$.size()');
>> 1

SELECT JSON_QUERY(JSON '2.3', '$.double()');
>> 2.3

SELECT JSON_QUERY(JSON '"2.3"', '$.double()');
>> 2.3

SELECT JSON_QUERY(JSON '2.3', '$.ceiling()');
>> 3

SELECT JSON_QUERY(JSON '3', '$.ceiling()');
>> 3

SELECT JSON_QUERY(JSON '2.3', '$.floor()');
>> 2

SELECT JSON_QUERY(JSON '3', '$.floor()');
>> 3

SELECT JSON_QUERY(JSON '100', '$.abs()');
>> 100

SELECT JSON_QUERY(JSON '-100', '$.abs()');
>> 100

SELECT JSON_QUERY(JSON '{"a": 1, "b": true}', '$.keyvalue().name' WITH ARRAY WRAPPER);
>> ["a","b"]

SELECT JSON_QUERY(JSON '[{"a": 1, "b": true}]', '$.keyvalue().name' WITH ARRAY WRAPPER);
>> ["a","b"]

SELECT JSON_QUERY(JSON '[{"a": 1, "b": true}]', 'strict $.keyvalue().name' WITH ARRAY WRAPPER ERROR ON ERROR);
> exception INVALID_VALUE_2

SELECT JSON_QUERY(JSON '{"a": 1, "b": true}', '$.keyvalue().value' WITH ARRAY WRAPPER);
>> [1,true]

SELECT JSON_QUERY(JSON '[[[1]]]', '$ ? (@ == 1)' EMPTY ARRAY ON EMPTY ERROR ON ERROR);
>> []

SELECT JSON_QUERY(JSON '[[[1]]]', '$ ? (1 == @)' EMPTY ARRAY ON EMPTY ERROR ON ERROR);
>> []

SELECT JSON_QUERY(JSON '[{"a" : 1}]', '$ ? (@ == 1)' EMPTY ARRAY ON EMPTY ERROR ON ERROR);
>> []

SELECT JSON_QUERY(JSON '[{"a" : 1}]', '$ ? (1 == @)' EMPTY ARRAY ON EMPTY ERROR ON ERROR);
>> []

SELECT JSON_QUERY(JSON '[1, null]', '$ ? (@ == null)' EMPTY ARRAY ON EMPTY ERROR ON ERROR);
>> null

SELECT JSON_QUERY(JSON '[1, null]', '$ ? (null <> @)' EMPTY ARRAY ON EMPTY ERROR ON ERROR);
>> 1

SELECT JSON_QUERY(JSON '[1, null]', '$ ? (@ > null)' EMPTY ARRAY ON EMPTY ERROR ON ERROR);
>> []

SELECT JSON_QUERY(JSON '[1, true, false, "2"]', '$ ? (@ == true)' EMPTY ARRAY ON EMPTY ERROR ON ERROR);
>> true

SELECT JSON_QUERY(JSON '[1, true, false, "2"]', '$ ? (@ == false)' EMPTY ARRAY ON EMPTY ERROR ON ERROR);
>> false

SELECT JSON_QUERY(JSON '[1, true, "2"]', '$ ? (@ == 1)' EMPTY ARRAY ON EMPTY ERROR ON ERROR);
>> 1

SELECT JSON_QUERY(JSON '[1, true, "2"]', '$ ? (@ == "2")' EMPTY ARRAY ON EMPTY ERROR ON ERROR);
>> "2"

SELECT JSON_QUERY(JSON '1', 'strict $ ? (@ == 1)');
>> 1

SELECT JSON_QUERY(JSON '[1, 2]', '$ ? (@ == 1)');
>> 1

SELECT JSON_QUERY(JSON '[1, 2]', 'strict $ ? (@ == 1)');
>> null

SELECT JSON_QUERY(JSON '[1, 2]', '$[*] ? (@ == 1)');
>> 1

SELECT JSON_QUERY(JSON '[1, 2]', '$[*] ? (@ <> 1)');
>> 2

SELECT JSON_QUERY(JSON '[1, 2]', '$[*] ? (@ != 1)');
>> 2

SELECT JSON_QUERY(JSON '[1, 2]', '$[*] ? (@ > 1)');
>> 2

SELECT JSON_QUERY(JSON '[1, 2]', '$[*] ? (@ < 2)');
>> 1

SELECT JSON_QUERY(JSON '[1, 2, 3]', '$[*] ? (@ >= 2)' WITH ARRAY WRAPPER);
>> [2,3]

SELECT JSON_QUERY(JSON '[1, 2, 3]', '$[*] ? (@ <= 2)' WITH ARRAY WRAPPER);
>> [1,2]

SELECT JSON_QUERY(JSON '[1, 2, 3]', '$[*] ? (@ > 1 && @ < 3)');
>> 2

SELECT JSON_QUERY(JSON '[1, 2, 3, 4]', '$[*] ? (@ < 2 || @ > 3)' WITH ARRAY WRAPPER);
>> [1,4]

SELECT JSON_QUERY(JSON '[1, 2, 3, null]', '$[*] ? (!(@ < 2))' WITH ARRAY WRAPPER);
>> [2,3]

SELECT JSON_QUERY(JSON '{"a" : 1, "b" : 1}', '$ ? ((@.a == @.b) is unknown)');
>> null

SELECT JSON_QUERY(JSON '{"a" : 1, "b" : true}', '$ ? ((@.a == @.b) is unknown)');
>> {"a":1,"b":true}

SELECT JSON_QUERY(JSON '{"a" : 1, "b" : 2}', '$ ? (exists(@.a))');
>> {"a":1,"b":2}

SELECT JSON_QUERY(JSON '{"a" : 1, "b" : 2}', '$ ? (exists(@.c))' EMPTY ARRAY ON EMPTY);
>> []

SELECT JSON_QUERY(JSON '["abc", "bdc"]', '$ ? (@ starts with "a")');
>> "abc"

SELECT JSON_QUERY(JSON '["abc", "bdc"]', 'strict $[*] ? (@ starts with "a")');
>> "abc"

SELECT JSON_QUERY(JSON '["abc", "bdc", 1]', '$ ? (@ starts with $P1)' PASSING JSON '"a"' AS P1);
>> "abc"

-- TODO should throw exception
SELECT JSON_QUERY(JSON '["abc", "bdc"]', '$ ? (@ starts with $P1)' PASSING JSON '"a"' AS P2);
>> null

SELECT JSON_QUERY(JSON '["abc", "bdc"]', '$ ? (@ starts with $P1)' PASSING JSON '1' AS P1);
>> null

SELECT JSON_QUERY(JSON '["abc", "Adc", "bdc"]', '$ ? (@ like_regex "a.*")');
>> "abc"

SELECT JSON_QUERY(JSON '["abc", "Adc", "bdc"]', 'strict $[*] ? (@ like_regex "a.*")');
>> "abc"

SELECT JSON_QUERY(JSON 'null', 'strict $ ? (@ like_regex "a.*")');
>> null

SELECT JSON_QUERY(JSON '"a\nb"', 'strict $ ? (@ like_regex "a.b")');
>> null

SELECT JSON_QUERY(JSON '"a\nb"', 'strict $ ? (@ like_regex "a.b" flag "s")');
>> "a\nb"

SELECT JSON_QUERY(JSON '"a\nb"', 'strict $ ? (@ like_regex "^a\nb$")');
>> "a\nb"

SELECT JSON_QUERY(JSON '"a\nb"', 'strict $ ? (@ like_regex "^a\nb$" flag "m")');
>> "a\nb"

SELECT JSON_QUERY(JSON '["abc", "Adc", "bdc"]', '$ ? (@ like_regex "a.*" flag "i")' WITH ARRAY WRAPPER);
>> ["abc","Adc"]

SELECT JSON_QUERY(JSON '"ab"', 'strict $ ? (@ like_regex "a b" flag "x")');
>> "ab"

SELECT JSON_QUERY(JSON '"[a]"', 'strict $ ? (@ like_regex "[a]" flag "q")');
>> "[a]"

SELECT JSON_QUERY(JSON '"[a]"', 'strict $ ? (@ like_regex "[a]" flag "?")');
> exception INVALID_VALUE_2

SELECT JSON_QUERY(JSON '"[2, 4]"', '$' KEEP QUOTES);
>> "[2, 4]"

SELECT JSON_QUERY(JSON '"[2, 4]"', '$' OMIT QUOTES);
>> [2,4]

SELECT JSON_QUERY(JSON '"!"', '$' OMIT QUOTES);
>> null

SELECT JSON_QUERY(JSON '1', '$' OMIT QUOTES);
>> 1

SELECT JSON_QUERY(JSON '[1, 2]', 'lax $[1 to 0]' EMPTY ARRAY ON EMPTY);
>> []

SELECT JSON_QUERY(JSON '[1, 2]', 'strict $[1 to 0]' EMPTY ARRAY ON EMPTY);
>> null

SELECT JSON_QUERY(JSON '[1, 2]', 'lax $[-1]' EMPTY ARRAY ON EMPTY);
>> []

SELECT JSON_QUERY(JSON '[1, 2]', 'strict $[-1]' EMPTY ARRAY ON EMPTY);
>> null

SELECT JSON_QUERY(JSON '[1, 2]', 'lax $[2]' EMPTY ARRAY ON EMPTY);
>> []

SELECT JSON_QUERY(JSON '[1, 2]', 'strict $[2]' EMPTY ARRAY ON EMPTY);
>> null

SELECT JSON_QUERY(JSON '[1, 2]', 'lax $[1 to 2]' EMPTY ARRAY ON EMPTY);
>> 2

SELECT JSON_QUERY(JSON '[1, 2]', 'strict $[1 to 2]' EMPTY ARRAY ON EMPTY);
>> null

SELECT JSON_QUERY(JSON '[1, 2]', '$[$[*]]' EMPTY ARRAY ON EMPTY);
>> null

SELECT JSON_QUERY(JSON '[1, 2]', '$[12345678901234567890]' EMPTY ARRAY ON EMPTY);
>> null

SELECT JSON_QUERY(JSON '[1, 2]', '$["a"]' EMPTY ARRAY ON EMPTY);
>> null

SELECT JSON_QUERY(JSON '1', '$ / 0' ERROR ON ERROR);
> exception DIVISION_BY_ZERO_1

SELECT JSON_QUERY(JSON '1', '$ % 0' ERROR ON ERROR);
> exception DIVISION_BY_ZERO_1

SELECT JSON_QUERY(JSON '[1, 2]', '$[*] * 3' ERROR ON ERROR);
> exception INVALID_VALUE_2

SELECT JSON_QUERY(JSON '"a"', '$ * 3' ERROR ON ERROR);
> exception INVALID_VALUE_2

SELECT JSON_QUERY(JSON '1', '@' ERROR ON ERROR);
> exception SYNTAX_ERROR_1

SELECT JSON_QUERY(JSON '[1]', 'last' ERROR ON ERROR);
> exception SYNTAX_ERROR_1

SELECT JSON_QUERY(JSON '1', '$.a' EMPTY ARRAY ON EMPTY ERROR ON ERROR);
>> []

SELECT JSON_QUERY(JSON '1', 'strict $.a' EMPTY ARRAY ON EMPTY ERROR ON ERROR);
> exception INVALID_VALUE_2

SELECT JSON_QUERY(JSON '{}', '$.a' EMPTY ARRAY ON EMPTY ERROR ON ERROR);
>> []

SELECT JSON_QUERY(JSON '{}', 'strict $.a' EMPTY ARRAY ON EMPTY ERROR ON ERROR);
> exception INVALID_VALUE_2

SELECT JSON_QUERY(JSON '{"a": 1}', '$.a');
>> 1

SELECT JSON_QUERY(JSON '{"a": 1}', 'strict $.a');
>> 1

SELECT JSON_QUERY(JSON '1', '$ + $P' ERROR ON ERROR);
> exception PARAMETER_NOT_SET_1

SELECT JSON_QUERY(JSON '1', 'strict $ + $P' ERROR ON ERROR);
> exception PARAMETER_NOT_SET_1

SELECT JSON_QUERY(JSON '[1, 2]', '$.double()' WITH ARRAY WRAPPER ERROR ON ERROR);
>> [1.0,2.0]

SELECT JSON_QUERY(JSON '[1, 2]', 'strict $.double()' WITH ARRAY WRAPPER ERROR ON ERROR);
> exception INVALID_VALUE_2

SELECT JSON_QUERY(JSON '"1:3"', '$.double()' ERROR ON ERROR);
> exception INVALID_VALUE_2

SELECT JSON_QUERY(JSON 'null', '$.double()' ERROR ON ERROR);
> exception INVALID_VALUE_2

SELECT JSON_QUERY(JSON 'null', 'strict $.double()' ERROR ON ERROR);
> exception INVALID_VALUE_2
