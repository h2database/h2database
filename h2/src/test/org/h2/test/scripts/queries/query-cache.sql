/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
-- Issue #4299: verify that uncommitted changes from the same transaction
-- invalidate cached result of EXISTS().
--
CREATE TABLE TEST(PK INTEGER PRIMARY KEY) AS SELECT VALUES ((1));
> ok

SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED;
> ok

@reconnect off

@autocommit off

SELECT ? FROM DUAL WHERE EXISTS(SELECT 1 FROM TEST WHERE PK = 1);
{
1
>> 1
};
> update count: 0

UPDATE TEST SET PK = 2 WHERE PK = 1;
> update count: 1

SELECT ? FROM DUAL WHERE EXISTS(SELECT 1 FROM TEST WHERE PK = 1);
{
1
>> <no result>
};
> update count: 0

@autocommit on

DROP TABLE TEST;
> ok
