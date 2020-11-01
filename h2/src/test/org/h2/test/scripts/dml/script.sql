-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

INSERT INTO TEST VALUES(2, STRINGDECODE('abcsond\344rzeich\344 ') || char(22222) || STRINGDECODE(' \366\344\374\326\304\334\351\350\340\361!'));
> update count: 1

SCRIPT NOPASSWORDS NOSETTINGS NOVERSION;
> SCRIPT
> ------------------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "ID" INTEGER NOT NULL, "NAME" CHARACTER VARYING(255) );
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_2" PRIMARY KEY("ID");
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> INSERT INTO "PUBLIC"."TEST" VALUES (2, U&'abcsond\00e4rzeich\00e4 \56ce \00f6\00e4\00fc\00d6\00c4\00dc\00e9\00e8\00e0\00f1!');
> rows (ordered): 5

SCRIPT COLUMNS NOPASSWORDS NOSETTINGS NOVERSION;
> SCRIPT
> --------------------------------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "ID" INTEGER NOT NULL, "NAME" CHARACTER VARYING(255) );
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_2" PRIMARY KEY("ID");
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> INSERT INTO "PUBLIC"."TEST"("ID", "NAME") VALUES (2, U&'abcsond\00e4rzeich\00e4 \56ce \00f6\00e4\00fc\00d6\00c4\00dc\00e9\00e8\00e0\00f1!');
> rows (ordered): 5

DROP TABLE TEST;
> ok

CREATE MEMORY TABLE TEST(ID BIGINT GENERATED ALWAYS AS IDENTITY, V INT, G INT GENERATED ALWAYS AS (V + 1));
> ok

INSERT INTO TEST(V) VALUES 5;
> update count: 1

SCRIPT NOPASSWORDS NOSETTINGS NOVERSION;
> SCRIPT
> -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "ID" BIGINT GENERATED ALWAYS AS IDENTITY( START WITH 1 RESTART WITH 2) NOT NULL, "V" INTEGER, "G" INTEGER GENERATED ALWAYS AS ("V" + 1) );
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> INSERT INTO "PUBLIC"."TEST"("ID", "V") OVERRIDING SYSTEM VALUE VALUES (1, 5);
> rows (ordered): 4

DROP TABLE TEST;
> ok

CREATE DOMAIN C AS INT;
> ok

CREATE DOMAIN B AS C;
> ok

CREATE DOMAIN A AS B;
> ok

SCRIPT NOPASSWORDS NOSETTINGS NOVERSION;
> SCRIPT
> -------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE DOMAIN "PUBLIC"."C" AS INTEGER;
> CREATE DOMAIN "PUBLIC"."B" AS "PUBLIC"."C";
> CREATE DOMAIN "PUBLIC"."A" AS "PUBLIC"."B";
> rows (ordered): 4

DROP DOMAIN A;
> ok

DROP DOMAIN B;
> ok

DROP DOMAIN C;
> ok
