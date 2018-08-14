-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(ID INT PRIMARY KEY,
    I01 INTERVAL YEAR, I02 INTERVAL MONTH, I03 INTERVAL DAY, I04 INTERVAL HOUR, I05 INTERVAL MINUTE,
    I06 INTERVAL SECOND, I07 INTERVAL YEAR TO MONTH, I08 INTERVAL DAY TO HOUR, I09 INTERVAL DAY TO MINUTE,
    I10 INTERVAL DAY TO SECOND, I11 INTERVAL HOUR TO MINUTE, I12 INTERVAL HOUR TO SECOND,
    I13 INTERVAL MINUTE TO SECOND,
    J01 INTERVAL YEAR(5), J02 INTERVAL MONTH(5), J03 INTERVAL DAY(5), J04 INTERVAL HOUR(5), J05 INTERVAL MINUTE(5),
    J06 INTERVAL SECOND(5, 9), J07 INTERVAL YEAR(5) TO MONTH, J08 INTERVAL DAY(5) TO HOUR,
    J09 INTERVAL DAY(5) TO MINUTE, J10 INTERVAL DAY(5) TO SECOND(9), J11 INTERVAL HOUR(5) TO MINUTE,
    J12 INTERVAL HOUR(5) TO SECOND(9), J13 INTERVAL MINUTE(5) TO SECOND(9));
> ok

SELECT COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION,
    INTERVAL_TYPE, INTERVAL_PRECISION
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE TYPE_NAME COLUMN_TYPE                     NUMERIC_PRECISION NUMERIC_SCALE DATETIME_PRECISION INTERVAL_TYPE          INTERVAL_PRECISION
> ----------- --------- --------- ------------------------------- ----------------- ------------- ------------------ ---------------------- ------------------
> ID          4         INTEGER   INT NOT NULL                    10                0             null               null                   null
> I01         1111      INTERVAL  INTERVAL YEAR                   2                 0             null               YEAR                   2
> I02         1111      INTERVAL  INTERVAL MONTH                  2                 0             null               MONTH                  2
> I03         1111      INTERVAL  INTERVAL DAY                    2                 0             null               DAY                    2
> I04         1111      INTERVAL  INTERVAL HOUR                   2                 0             null               HOUR                   2
> I05         1111      INTERVAL  INTERVAL MINUTE                 2                 0             null               MINUTE                 2
> I06         1111      INTERVAL  INTERVAL SECOND                 2                 6             6                  SECOND                 2
> I07         1111      INTERVAL  INTERVAL YEAR TO MONTH          2                 0             null               YEAR TO MONTH          2
> I08         1111      INTERVAL  INTERVAL DAY TO HOUR            2                 0             null               DAY TO HOUR            2
> I09         1111      INTERVAL  INTERVAL DAY TO MINUTE          2                 0             null               DAY TO MINUTE          2
> I10         1111      INTERVAL  INTERVAL DAY TO SECOND          2                 6             6                  DAY TO SECOND          2
> I11         1111      INTERVAL  INTERVAL HOUR TO MINUTE         2                 0             null               HOUR TO MINUTE         2
> I12         1111      INTERVAL  INTERVAL HOUR TO SECOND         2                 6             6                  HOUR TO SECOND         2
> I13         1111      INTERVAL  INTERVAL MINUTE TO SECOND       2                 6             6                  MINUTE TO SECOND       2
> J01         1111      INTERVAL  INTERVAL YEAR(5)                5                 0             null               YEAR(5)                5
> J02         1111      INTERVAL  INTERVAL MONTH(5)               5                 0             null               MONTH(5)               5
> J03         1111      INTERVAL  INTERVAL DAY(5)                 5                 0             null               DAY(5)                 5
> J04         1111      INTERVAL  INTERVAL HOUR(5)                5                 0             null               HOUR(5)                5
> J05         1111      INTERVAL  INTERVAL MINUTE(5)              5                 0             null               MINUTE(5)              5
> J06         1111      INTERVAL  INTERVAL SECOND(5, 9)           5                 9             9                  SECOND(5, 9)           5
> J07         1111      INTERVAL  INTERVAL YEAR(5) TO MONTH       5                 0             null               YEAR(5) TO MONTH       5
> J08         1111      INTERVAL  INTERVAL DAY(5) TO HOUR         5                 0             null               DAY(5) TO HOUR         5
> J09         1111      INTERVAL  INTERVAL DAY(5) TO MINUTE       5                 0             null               DAY(5) TO MINUTE       5
> J10         1111      INTERVAL  INTERVAL DAY(5) TO SECOND(9)    5                 9             9                  DAY(5) TO SECOND(9)    5
> J11         1111      INTERVAL  INTERVAL HOUR(5) TO MINUTE      5                 0             null               HOUR(5) TO MINUTE      5
> J12         1111      INTERVAL  INTERVAL HOUR(5) TO SECOND(9)   5                 9             9                  HOUR(5) TO SECOND(9)   5
> J13         1111      INTERVAL  INTERVAL MINUTE(5) TO SECOND(9) 5                 9             9                  MINUTE(5) TO SECOND(9) 5
> rows (ordered): 27

INSERT INTO TEST VALUES (
    1,
    INTERVAL '1' YEAR, INTERVAL '1' MONTH, INTERVAL '1' DAY, INTERVAL '1' HOUR, INTERVAL '1' MINUTE,
    INTERVAL '1.123456789' SECOND, INTERVAL '1-2' YEAR TO MONTH, INTERVAL '1 2' DAY TO HOUR,
    INTERVAL '1 2:3' DAY TO MINUTE, INTERVAL '1 2:3:4.123456789' DAY TO SECOND, INTERVAL '1:2' HOUR TO MINUTE,
    INTERVAL '1:2:3.123456789' HOUR TO SECOND, INTERVAL '1:2.123456789' MINUTE TO SECOND,
    INTERVAL '1' YEAR, INTERVAL '1' MONTH, INTERVAL '1' DAY, INTERVAL '1' HOUR, INTERVAL '1' MINUTE,
    INTERVAL '1.123456789' SECOND, INTERVAL '1-2' YEAR TO MONTH, INTERVAL '1 2' DAY TO HOUR,
    INTERVAL '1 2:3' DAY TO MINUTE, INTERVAL '1 2:3:4.123456789' DAY TO SECOND, INTERVAL '1:2' HOUR TO MINUTE,
    INTERVAL '1:2:3.123456789' HOUR TO SECOND, INTERVAL '1:2.123456789' MINUTE TO SECOND
    ), (
    2,
    INTERVAL '-1' YEAR, INTERVAL '-1' MONTH, INTERVAL '-1' DAY, INTERVAL '-1' HOUR, INTERVAL '-1' MINUTE,
    INTERVAL '-1.123456789' SECOND, INTERVAL '-1-2' YEAR TO MONTH, INTERVAL '-1 2' DAY TO HOUR,
    INTERVAL '-1 2:3' DAY TO MINUTE, INTERVAL '-1 2:3:4.123456789' DAY TO SECOND, INTERVAL '-1:2' HOUR TO MINUTE,
    INTERVAL '-1:2:3.123456789' HOUR TO SECOND, INTERVAL '-1:2.123456789' MINUTE TO SECOND,
    INTERVAL -'1' YEAR, INTERVAL -'1' MONTH, INTERVAL -'1' DAY, INTERVAL -'1' HOUR, INTERVAL -'1' MINUTE,
    INTERVAL -'1.123456789' SECOND, INTERVAL -'1-2' YEAR TO MONTH, INTERVAL -'1 2' DAY TO HOUR,
    INTERVAL -'1 2:3' DAY TO MINUTE, INTERVAL -'1 2:3:4.123456789' DAY TO SECOND, INTERVAL -'1:2' HOUR TO MINUTE,
    INTERVAL -'1:2:3.123456789' HOUR TO SECOND, INTERVAL -'1:2.123456789' MINUTE TO SECOND);
> update count: 2

SELECT I01, I02, I03, I04, I05, I06 FROM TEST ORDER BY ID;
> I01                I02                 I03               I04                I05                  I06
> ------------------ ------------------- ----------------- ------------------ -------------------- --------------------------
> INTERVAL '1' YEAR  INTERVAL '1' MONTH  INTERVAL '1' DAY  INTERVAL '1' HOUR  INTERVAL '1' MINUTE  INTERVAL '1.123457' SECOND
> INTERVAL '-1' YEAR INTERVAL '-1' MONTH INTERVAL '-1' DAY INTERVAL '-1' HOUR INTERVAL '-1' MINUTE INTERVAL '1.123457' SECOND
> rows (ordered): 2

SELECT I07, I08, I09, I10 FROM TEST ORDER BY ID;
> I07                           I08                          I09                               I10
> ----------------------------- ---------------------------- --------------------------------- ------------------------------------------
> INTERVAL '1-2' YEAR TO MONTH  INTERVAL '1 02' DAY TO HOUR  INTERVAL '1 02:03' DAY TO MINUTE  INTERVAL '1 02:03:04.123457' DAY TO SECOND
> INTERVAL '-1-2' YEAR TO MONTH INTERVAL '-1 02' DAY TO HOUR INTERVAL '-1 02:03' DAY TO MINUTE INTERVAL '1 02:03:04.123457' DAY TO SECOND
> rows (ordered): 2

SELECT I11, I12, I12 FROM TEST ORDER BY ID;
> I11                             I12                                      I12
> ------------------------------- ---------------------------------------- ----------------------------------------
> INTERVAL '1:02' HOUR TO MINUTE  INTERVAL '1:02:03.123457' HOUR TO SECOND INTERVAL '1:02:03.123457' HOUR TO SECOND
> INTERVAL '-1:02' HOUR TO MINUTE INTERVAL '1:02:03.123457' HOUR TO SECOND INTERVAL '1:02:03.123457' HOUR TO SECOND
> rows (ordered): 2

SELECT J01, J02, J03, J04, J05, J06 FROM TEST ORDER BY ID;
> J01                J02                 J03               J04                J05                  J06
> ------------------ ------------------- ----------------- ------------------ -------------------- ------------------------------
> INTERVAL '1' YEAR  INTERVAL '1' MONTH  INTERVAL '1' DAY  INTERVAL '1' HOUR  INTERVAL '1' MINUTE  INTERVAL '1.123456789' SECOND
> INTERVAL '-1' YEAR INTERVAL '-1' MONTH INTERVAL '-1' DAY INTERVAL '-1' HOUR INTERVAL '-1' MINUTE INTERVAL '-1.123456789' SECOND
> rows (ordered): 2

SELECT J07, J08, J09, J10 FROM TEST ORDER BY ID;
> J07                           J08                          J09                               J10
> ----------------------------- ---------------------------- --------------------------------- ----------------------------------------------
> INTERVAL '1-2' YEAR TO MONTH  INTERVAL '1 02' DAY TO HOUR  INTERVAL '1 02:03' DAY TO MINUTE  INTERVAL '1 02:03:04.123456789' DAY TO SECOND
> INTERVAL '-1-2' YEAR TO MONTH INTERVAL '-1 02' DAY TO HOUR INTERVAL '-1 02:03' DAY TO MINUTE INTERVAL '-1 02:03:04.123456789' DAY TO SECOND
> rows (ordered): 2

SELECT J11, J12, J12 FROM TEST ORDER BY ID;
> J11                             J12                                          J12
> ------------------------------- -------------------------------------------- --------------------------------------------
> INTERVAL '1:02' HOUR TO MINUTE  INTERVAL '1:02:03.123456789' HOUR TO SECOND  INTERVAL '1:02:03.123456789' HOUR TO SECOND
> INTERVAL '-1:02' HOUR TO MINUTE INTERVAL '-1:02:03.123456789' HOUR TO SECOND INTERVAL '-1:02:03.123456789' HOUR TO SECOND
> rows (ordered): 2

DROP TABLE TEST;
> ok
