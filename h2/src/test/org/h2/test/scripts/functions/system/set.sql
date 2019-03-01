-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

@reconnect off

-- Try a custom column naming rules setup

SET COLUMN_NAME_RULES=MAX_IDENTIFIER_LENGTH = 30;
> exception INVALID_VALUE_2

SET COLUMN_NAME_RULES=REGULAR_EXPRESSION_MATCH_ALLOWED = '[A-Za-z0-9_]+';
> exception INVALID_VALUE_2

SET COLUMN_NAME_RULES=REGULAR_EXPRESSION_MATCH_DISALLOWED = '[^A-Za-z0-9_]+';
> exception INVALID_VALUE_2

SET COLUMN_NAME_RULES=DEFAULT_COLUMN_NAME_PATTERN = 'noName$$';
> exception INVALID_VALUE_2

SET COLUMN_NAME_RULES=GENERATE_UNIQUE_COLUMN_NAMES = 1;
> exception INVALID_VALUE_2

SELECT 1 AS VERY_VERY_VERY_LONG_ID_VERY_VERY_VERY_LONG_ID, SUM(X)+1 AS _123456789012345, SUM(X)+1 , SUM(X)+1
+47, 'x' , '!!!' , '!!!!' FROM SYSTEM_RANGE(1,2);
> VERY_VERY_VERY_LONG_ID_VERY_VERY_VERY_LONG_ID _123456789012345 SUM("X") + 1 (SUM("X") + 1) + 47 'x' '!!!' '!!!!'
> --------------------------------------------- ---------------- ------------ ------------------- --- ----- ------
> 1                                             4                4            51                  x   !!!   !!!!
> rows: 1

SET COLUMN_NAME_RULES=EMULATE='Oracle';
> exception INVALID_VALUE_2

SELECT 1 AS VERY_VERY_VERY_LONG_ID, SUM(X)+1 AS _123456789012345, SUM(X)+1 , SUM(X)+1
+47, 'x' , '!!!' , '!!!!' FROM SYSTEM_RANGE(1,2);
> VERY_VERY_VERY_LONG_ID _123456789012345 SUM("X") + 1 (SUM("X") + 1) + 47 'x' '!!!' '!!!!'
> ---------------------- ---------------- ------------ ------------------- --- ----- ------
> 1                      4                4            51                  x   !!!   !!!!
> rows: 1

SET COLUMN_NAME_RULES=EMULATE='Oracle';
> exception INVALID_VALUE_2

SELECT 1 AS VERY_VERY_VERY_LONG_ID, SUM(X)+1 AS _123456789012345, SUM(X)+1 , SUM(X)+1
+47, 'x' , '!!!' , '!!!!', 'Very Long' AS _23456789012345678901234567890XXX FROM SYSTEM_RANGE(1,2);
> VERY_VERY_VERY_LONG_ID _123456789012345 SUM("X") + 1 (SUM("X") + 1) + 47 'x' '!!!' '!!!!' _23456789012345678901234567890XXX
> ---------------------- ---------------- ------------ ------------------- --- ----- ------ ---------------------------------
> 1                      4                4            51                  x   !!!   !!!!   Very Long
> rows: 1

SET COLUMN_NAME_RULES=EMULATE='PostgreSQL';
> exception INVALID_VALUE_2

SELECT 1 AS VERY_VERY_VERY_LONG_ID, SUM(X)+1 AS _123456789012345, SUM(X)+1 , SUM(X)+1
+47, 'x' , '!!!' , '!!!!', 999 AS "QuotedColumnId" FROM SYSTEM_RANGE(1,2);
> VERY_VERY_VERY_LONG_ID _123456789012345 SUM("X") + 1 (SUM("X") + 1) + 47 'x' '!!!' '!!!!' QuotedColumnId
> ---------------------- ---------------- ------------ ------------------- --- ----- ------ --------------
> 1                      4                4            51                  x   !!!   !!!!   999
> rows: 1

SET COLUMN_NAME_RULES=DEFAULT;
> ok

-- Test all MODES of database:
-- DB2, Derby, MSSQLServer, HSQLDB, MySQL, Oracle, PostgreSQL, Ignite
SET COLUMN_NAME_RULES=EMULATE='DB2';
> exception INVALID_VALUE_2

SET COLUMN_NAME_RULES=EMULATE='Derby';
> exception INVALID_VALUE_2

SET COLUMN_NAME_RULES=EMULATE='MSSQLServer';
> exception INVALID_VALUE_2

SET COLUMN_NAME_RULES=EMULATE='MySQL';
> exception INVALID_VALUE_2

SET COLUMN_NAME_RULES=EMULATE='Oracle';
> exception INVALID_VALUE_2

SET COLUMN_NAME_RULES=EMULATE='PostgreSQL';
> exception INVALID_VALUE_2

SET COLUMN_NAME_RULES=EMULATE='Ignite';
> exception INVALID_VALUE_2

SET COLUMN_NAME_RULES=EMULATE='REGULAR';
> exception INVALID_VALUE_2
