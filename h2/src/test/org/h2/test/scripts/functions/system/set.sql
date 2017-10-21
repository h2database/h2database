-- Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--
-- Try a custom column naming rules setup

SET COLUMN_NAME_RULES=MAX_IDENTIFIER_LENGTH = 10;
> ok

SET COLUMN_NAME_RULES=REGULAR_EXPRESSION_MATCH_ALLOWED = '[A-Za-z0-9_]+';
> ok

SET COLUMN_NAME_RULES=REGULAR_EXPRESSION_MATCH_DISALLOWED = '[^A-Za-z0-9_]+';
> ok

SET COLUMN_NAME_RULES=DEFAULT_COLUMN_NAME_PATTERN = 'noName$$';
> ok

SELECT 1 AS VERY_VERY_VERY_LONG_ID, SUM(X)+1 AS _123456789012345, SUM(X)+1 , SUM(X)+1 
+47, 'x' , '!!!' , '!!!!' FROM SYSTEM_RANGE(1,2);
> VERY_VERY_ _123456789 SUMX1 SUMX147 x noName6 noName7
> ---------- ---------- ----- ------- - ------- -------
> 1          4          4     51      x !!!     !!!!

SET COLUMN_NAME_RULES=EMULATE=ORACLE128;
> ok

SELECT 1 AS VERY_VERY_VERY_LONG_ID, SUM(X)+1 AS _123456789012345, SUM(X)+1 , SUM(X)+1 
+47, 'x' , '!!!' , '!!!!' FROM SYSTEM_RANGE(1,2);
> VERY_VERY_VERY_LONG_ID _123456789012345 SUMX1 SUMX147 x _UNNAMED_6 _UNNAMED_7
> ---------------------- ---------------- ----- ------- - ---------- ----------
> 1                      4                4     51      x !!!        !!!!

SET COLUMN_NAME_RULES=EMULATE=ORACLE30;
> ok

SELECT 1 AS VERY_VERY_VERY_LONG_ID, SUM(X)+1 AS _123456789012345, SUM(X)+1 , SUM(X)+1 
+47, 'x' , '!!!' , '!!!!', 'Very Long' AS _23456789012345678901234567890XXX FROM SYSTEM_RANGE(1,2);
> VERY_VERY_VERY_LONG_ID _123456789012345 SUMX1 SUMX147 x _UNNAMED_6 _UNNAMED_7 _23456789012345678901234567890
> ---------------------- ---------------- ----- ------- - ---------- ---------- ------------------------------
> 1                      4                4     51      x !!!        !!!!       Very Long

SET COLUMN_NAME_RULES=EMULATE=POSTGRES;
> ok

SELECT 1 AS VERY_VERY_VERY_LONG_ID, SUM(X)+1 AS _123456789012345, SUM(X)+1 , SUM(X)+1 
+47, 'x' , '!!!' , '!!!!', 999 AS "QuotedColumnId" FROM SYSTEM_RANGE(1,2);
> VERY_VERY_VERY_LONG_ID _123456789012345 SUMX1 SUMX147 x _UNNAMED_6 _UNNAMED_7 QuotedColumnId
> ---------------------- ---------------- ----- ------- - ---------- ---------- --------------
> 1                      4                4     51      x !!!        !!!!       999

SET COLUMN_NAME_RULES=DEFAULT;
> ok
