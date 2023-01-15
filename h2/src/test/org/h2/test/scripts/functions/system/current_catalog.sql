-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CALL CURRENT_CATALOG;
>> SCRIPT

CALL DATABASE();
>> SCRIPT

SET CATALOG SCRIPT;
> ok

SET CATALOG 'SCRIPT';
> ok

SET CATALOG 'SCR' || 'IPT';
> ok

SET CATALOG UNKNOWN_CATALOG;
> exception DATABASE_NOT_FOUND_1

SET CATALOG NULL;
> exception DATABASE_NOT_FOUND_1

CALL CURRENT_DATABASE();
> exception FUNCTION_NOT_FOUND_1

SET MODE PostgreSQL;
> ok

CALL CURRENT_DATABASE();
>> SCRIPT

SET MODE Regular;
> ok
