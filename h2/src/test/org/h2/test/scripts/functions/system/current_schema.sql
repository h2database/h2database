-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT CURRENT_SCHEMA, SCHEMA();
> CURRENT_SCHEMA CURRENT_SCHEMA
> -------------- --------------
> PUBLIC         PUBLIC
> rows: 1

CREATE SCHEMA S1;
> ok

SET SCHEMA S1;
> ok

CALL CURRENT_SCHEMA;
>> S1

SET SCHEMA 'PUBLIC';
> ok

CALL CURRENT_SCHEMA;
>> PUBLIC

SET SCHEMA 'S' || 1;
> ok

CALL CURRENT_SCHEMA;
>> S1

SET SCHEMA PUBLIC;
> ok

SET SCHEMA NULL;
> exception SCHEMA_NOT_FOUND_1

DROP SCHEMA S1;
> ok
