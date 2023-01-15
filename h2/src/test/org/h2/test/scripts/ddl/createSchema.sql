-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE USER TEST_USER PASSWORD 'test';
> ok

CREATE ROLE TEST_ROLE;
> ok

CREATE SCHEMA S1;
> ok

CREATE SCHEMA S2 AUTHORIZATION TEST_USER;
> ok

CREATE SCHEMA S3 AUTHORIZATION TEST_ROLE;
> ok

CREATE SCHEMA AUTHORIZATION TEST_USER;
> ok

CREATE SCHEMA AUTHORIZATION TEST_ROLE;
> ok

TABLE INFORMATION_SCHEMA.SCHEMATA;
> CATALOG_NAME SCHEMA_NAME        SCHEMA_OWNER DEFAULT_CHARACTER_SET_CATALOG DEFAULT_CHARACTER_SET_SCHEMA DEFAULT_CHARACTER_SET_NAME SQL_PATH DEFAULT_COLLATION_NAME REMARKS
> ------------ ------------------ ------------ ----------------------------- ---------------------------- -------------------------- -------- ---------------------- -------
> SCRIPT       INFORMATION_SCHEMA SA           SCRIPT                        PUBLIC                       Unicode                    null     OFF                    null
> SCRIPT       PUBLIC             SA           SCRIPT                        PUBLIC                       Unicode                    null     OFF                    null
> SCRIPT       S1                 SA           SCRIPT                        PUBLIC                       Unicode                    null     OFF                    null
> SCRIPT       S2                 TEST_USER    SCRIPT                        PUBLIC                       Unicode                    null     OFF                    null
> SCRIPT       S3                 TEST_ROLE    SCRIPT                        PUBLIC                       Unicode                    null     OFF                    null
> SCRIPT       TEST_ROLE          TEST_ROLE    SCRIPT                        PUBLIC                       Unicode                    null     OFF                    null
> SCRIPT       TEST_USER          TEST_USER    SCRIPT                        PUBLIC                       Unicode                    null     OFF                    null
> rows: 7

DROP SCHEMA S1;
> ok

DROP SCHEMA S2;
> ok

DROP SCHEMA S3;
> ok

DROP USER TEST_USER;
> exception CANNOT_DROP_2

DROP ROLE TEST_ROLE;
> exception CANNOT_DROP_2

DROP SCHEMA TEST_USER;
> ok

DROP SCHEMA TEST_ROLE;
> ok

DROP USER TEST_USER;
> ok

DROP ROLE TEST_ROLE;
> ok
