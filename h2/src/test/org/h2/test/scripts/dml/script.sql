-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

INSERT INTO TEST VALUES(2, STRINGDECODE('abcsond\344rzeich\344 ') || char(22222) || STRINGDECODE(' \366\344\374\326\304\334\351\350\340\361!'));
> update count: 1

SCRIPT NOPASSWORDS NOSETTINGS;
> SCRIPT
> ------------------------------------------------------------------------------------------------------------------------------
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_2" PRIMARY KEY("ID");
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "ID" INTEGER NOT NULL, "NAME" CHARACTER VARYING(255) );
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> INSERT INTO "PUBLIC"."TEST" VALUES (2, U&'abcsond\00e4rzeich\00e4 \56ce \00f6\00e4\00fc\00d6\00c4\00dc\00e9\00e8\00e0\00f1!');
> rows: 5

SCRIPT COLUMNS NOPASSWORDS NOSETTINGS;
> SCRIPT
> --------------------------------------------------------------------------------------------------------------------------------------------
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_2" PRIMARY KEY("ID");
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "ID" INTEGER NOT NULL, "NAME" CHARACTER VARYING(255) );
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> INSERT INTO "PUBLIC"."TEST"("ID", "NAME") VALUES (2, U&'abcsond\00e4rzeich\00e4 \56ce \00f6\00e4\00fc\00d6\00c4\00dc\00e9\00e8\00e0\00f1!');
> rows: 5

DROP TABLE TEST;
> ok

CREATE MEMORY TABLE TEST(ID BIGINT GENERATED ALWAYS AS IDENTITY, V INT, G INT GENERATED ALWAYS AS (V + 1));
> ok

INSERT INTO TEST(V) VALUES 5;
> update count: 1

SCRIPT NOPASSWORDS NOSETTINGS;
> SCRIPT
> -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "ID" BIGINT GENERATED ALWAYS AS IDENTITY( START WITH 1 RESTART WITH 2) NOT NULL, "V" INTEGER, "G" INTEGER GENERATED ALWAYS AS ("V" + 1) );
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> INSERT INTO "PUBLIC"."TEST"("ID", "V") OVERRIDING SYSTEM VALUE VALUES (1, 5);
> rows: 4

DROP TABLE TEST;
> ok
