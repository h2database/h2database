-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE SCHEMA S1;
> ok

CREATE SCHEMA S2;
> ok

CREATE DOMAIN S1.D1 AS INT DEFAULT 1;
> ok

CREATE DOMAIN S2.D2 AS TIMESTAMP WITH TIME ZONE ON UPDATE CURRENT_TIMESTAMP;
> ok

CREATE TABLE TEST(C1 S1.D1, C2 S2.D2);
> ok

SELECT COLUMN_NAME, DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME, COLUMN_DEFAULT, COLUMN_TYPE, COLUMN_ON_UPDATE
    FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DOMAIN_CATALOG DOMAIN_SCHEMA DOMAIN_NAME COLUMN_DEFAULT COLUMN_TYPE                           COLUMN_ON_UPDATE
> ----------- -------------- ------------- ----------- -------------- ------------------------------------- -----------------
> C1          SCRIPT         S1            D1          1              "S1"."D1" DEFAULT 1                   null
> C2          SCRIPT         S2            D2          null           "S2"."D2" ON UPDATE CURRENT_TIMESTAMP CURRENT_TIMESTAMP
> rows (ordered): 2

SELECT DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME, DOMAIN_DEFAULT, DOMAIN_ON_UPDATE, TYPE_NAME FROM INFORMATION_SCHEMA.DOMAINS;
> DOMAIN_CATALOG DOMAIN_SCHEMA DOMAIN_NAME DOMAIN_DEFAULT DOMAIN_ON_UPDATE  TYPE_NAME
> -------------- ------------- ----------- -------------- ----------------- ------------------------
> SCRIPT         S1            D1          1              null              INTEGER
> SCRIPT         S2            D2          null           CURRENT_TIMESTAMP TIMESTAMP WITH TIME ZONE
> rows: 2

DROP TABLE TEST;
> ok

DROP DOMAIN S1.D1;
> ok

DROP SCHEMA S1 RESTRICT;
> ok

DROP SCHEMA S2 RESTRICT;
> exception CANNOT_DROP_2

DROP SCHEMA S2 CASCADE;
> ok
