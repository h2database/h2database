-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE SCHEMA CONST;
> ok

CREATE CONSTANT IF NOT EXISTS ONE VALUE 1;
> ok

COMMENT ON CONSTANT ONE IS 'Eins';
> ok

CREATE CONSTANT IF NOT EXISTS ONE VALUE 1;
> ok

CREATE CONSTANT CONST.ONE VALUE 1;
> ok

SELECT CONSTANT_SCHEMA, CONSTANT_NAME, DATA_TYPE, NUMERIC_PRECISION, REMARKS, SQL FROM INFORMATION_SCHEMA.CONSTANTS;
> CONSTANT_SCHEMA CONSTANT_NAME DATA_TYPE NUMERIC_PRECISION REMARKS SQL
> --------------- ------------- --------- ----------------- ------- ---
> CONST           ONE           INTEGER   32                null    1
> PUBLIC          ONE           INTEGER   32                Eins    1
> rows: 2

SELECT ONE, CONST.ONE;
> 1 1
> - -
> 1 1
> rows: 1

COMMENT ON CONSTANT ONE IS NULL;
> ok

DROP SCHEMA CONST CASCADE;
> ok

SELECT CONSTANT_SCHEMA, CONSTANT_NAME, DATA_TYPE, REMARKS, SQL FROM INFORMATION_SCHEMA.CONSTANTS;
> CONSTANT_SCHEMA CONSTANT_NAME DATA_TYPE REMARKS SQL
> --------------- ------------- --------- ------- ---
> PUBLIC          ONE           INTEGER   null    1
> rows: 1

DROP CONSTANT ONE;
> ok

DROP CONSTANT IF EXISTS ONE;
> ok

create constant abc value 1;
> ok

call abc;
> 1
> -
> 1
> rows: 1

drop all objects;
> ok

call abc;
> exception COLUMN_NOT_FOUND_1

create constant abc value 1;
> ok

comment on constant abc is 'One';
> ok

select remarks from information_schema.constants where constant_name = 'ABC';
>> One

@reconnect

select remarks from information_schema.constants where constant_name = 'ABC';
>> One

drop constant abc;
> ok
