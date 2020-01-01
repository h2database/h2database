-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE DOMAIN E AS ENUM('A', 'B');
> ok

CREATE TABLE TEST(I INT PRIMARY KEY, E1 E, E2 E NOT NULL);
> ok

INSERT INTO TEST VALUES (1, 'A', 'B');
> update count: 1

SELECT COLUMN_NAME, DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME, NULLABLE, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DOMAIN_CATALOG DOMAIN_SCHEMA DOMAIN_NAME NULLABLE COLUMN_TYPE
> ----------- -------------- ------------- ----------- -------- ---------------------
> I           null           null          null        0        INT NOT NULL
> E1          SCRIPT         PUBLIC        E           1        "PUBLIC"."E"
> E2          SCRIPT         PUBLIC        E           0        "PUBLIC"."E" NOT NULL
> rows (ordered): 3

DROP DOMAIN E RESTRICT;
> exception CANNOT_DROP_2

DROP DOMAIN E CASCADE;
> ok

SELECT COLUMN_NAME, DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME, IS_NULLABLE, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DOMAIN_CATALOG DOMAIN_SCHEMA DOMAIN_NAME IS_NULLABLE COLUMN_TYPE
> ----------- -------------- ------------- ----------- ----------- -----------------------
> I           null           null          null        NO          INT NOT NULL
> E1          null           null          null        YES         ENUM('A', 'B')
> E2          null           null          null        NO          ENUM('A', 'B') NOT NULL
> rows (ordered): 3

DROP TABLE TEST;
> ok

CREATE DOMAIN D INT CHECK (VALUE > 0);
> ok

CREATE MEMORY TABLE TEST(C D);
> ok

DROP DOMAIN D CASCADE;
> ok

INSERT INTO TEST VALUES 1;
> update count: 1

INSERT INTO TEST VALUES -1;
> exception CHECK_CONSTRAINT_VIOLATED_1

@reconnect

INSERT INTO TEST VALUES 1;
> update count: 1

INSERT INTO TEST VALUES -1;
> exception CHECK_CONSTRAINT_VIOLATED_1

SCRIPT NOPASSWORDS NOSETTINGS TABLE TEST;
> SCRIPT
> ------------------------------------------------------------------------------------------
> -- 2 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_2" CHECK("C" > 0) NOCHECK;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "C" INT );
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> INSERT INTO "PUBLIC"."TEST" VALUES (1), (1);
> rows: 5

DROP TABLE TEST;
> ok
