-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(S VARCHAR(10), B VARBINARY(10), A VARCHAR(10) ARRAY) AS VALUES
    ('a', X'49', ARRAY['b']), ('', X'', ARRAY[]), (NULL, NULL, NULL);
> ok

EXPLAIN SELECT S || 'v' || '' || 'x' || S || (S || S), S || '', S || (B || X'50'), B || B || B FROM TEST;
>> SELECT "S" || 'vx' || "S" || "S" || "S", "S", "S" || ("B" || X'50'), "B" || "B" || "B" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

SELECT S || 'v' || '' || 'x' || S || (S || S), S || '', S || (B || X'50'), B || B || B FROM TEST;
> S || 'vx' || S || S || S S    S || (B || X'50') B || B || B
> ------------------------ ---- ----------------- -----------
> avxaaa                   a    aIP               X'494949'
> null                     null null              null
> vx                            P                 X''
> rows: 3

EXPLAIN SELECT S || A, ARRAY[] || A, S || CAST(ARRAY[] AS VARCHAR ARRAY), A || A || A FROM TEST;
>> SELECT "S" || "A", "A", CAST("S" AS CHARACTER VARYING ARRAY), "A" || "A" || "A" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

SELECT S || A, ARRAY[] || A, S || CAST(ARRAY[] AS VARCHAR ARRAY), A || A || A FROM TEST;
> S || A A    CAST(S AS CHARACTER VARYING ARRAY) A || A || A
> ------ ---- ---------------------------------- -----------
> []     []   []                                 []
> [a, b] [b]  [a]                                [b, b, b]
> null   null null                               null
> rows: 3

EXPLAIN SELECT B || NULL, B || X'22' || NULL FROM TEST;
>> SELECT CAST(NULL AS BINARY VARYING(10)), CAST(NULL AS BINARY VARYING(11)) FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

SELECT B || NULL, B || X'22' || NULL FROM TEST;
> CAST(NULL AS BINARY VARYING(10)) CAST(NULL AS BINARY VARYING(11))
> -------------------------------- --------------------------------
> null                             null
> null                             null
> null                             null
> rows: 3

EXPLAIN SELECT B || X'', A || ARRAY['a'] FROM TEST;
>> SELECT "B", "A" || ARRAY ['a'] FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

EXPLAIN SELECT (S || S) || (B || B) FROM TEST;
>> SELECT "S" || "S" || ("B" || "B") FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

DROP TABLE TEST;
> ok
