-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(I01 INTERVAL YEAR, I02 INTERVAL MONTH, I03 INTERVAL DAY, I04 INTERVAL HOUR, I05 INTERVAL MINUTE,
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
> J06         1111      INTERVAL  INTERVAL SECOND(5, 9)           5                 6             6                  SECOND(5, 9)           5
> J07         1111      INTERVAL  INTERVAL YEAR(5) TO MONTH       5                 0             null               YEAR(5) TO MONTH       5
> J08         1111      INTERVAL  INTERVAL DAY(5) TO HOUR         5                 0             null               DAY(5) TO HOUR         5
> J09         1111      INTERVAL  INTERVAL DAY(5) TO MINUTE       5                 0             null               DAY(5) TO MINUTE       5
> J10         1111      INTERVAL  INTERVAL DAY(5) TO SECOND(9)    5                 6             6                  DAY(5) TO SECOND(9)    5
> J11         1111      INTERVAL  INTERVAL HOUR(5) TO MINUTE      5                 0             null               HOUR(5) TO MINUTE      5
> J12         1111      INTERVAL  INTERVAL HOUR(5) TO SECOND(9)   5                 6             6                  HOUR(5) TO SECOND(9)   5
> J13         1111      INTERVAL  INTERVAL MINUTE(5) TO SECOND(9) 5                 6             6                  MINUTE(5) TO SECOND(9) 5
> rows (ordered): 26

DROP TABLE TEST;
> ok
