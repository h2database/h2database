-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT JSON_ARRAY(10, TRUE, 'str', NULL, '[1,2,3]'::JSON);
>> [10,true,"str",null,[1,2,3]]

SELECT JSON_ARRAY(NULL ABSENT ON NULL);
>> []

SELECT JSON_ARRAY(NULL NULL ON NULL);
>> [null]

CREATE TABLE TEST(V VARCHAR);
> ok

EXPLAIN SELECT JSON_ARRAY(V NULL ON NULL) FROM TEST;
>> SELECT JSON_ARRAY("V") FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

EXPLAIN SELECT JSON_ARRAY(V ABSENT ON NULL) FROM TEST;
>> SELECT JSON_ARRAY("V" ABSENT ON NULL) FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

DROP TABLE TEST;
> ok
