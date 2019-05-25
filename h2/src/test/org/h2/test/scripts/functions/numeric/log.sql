-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT LN(NULL), LOG(NULL, NULL), LOG(NULL, 2), LOG(2, NULL), LOG10(NULL), LOG(NULL);
> NULL NULL NULL NULL NULL NULL
> ---- ---- ---- ---- ---- ----
> null null null null null null
> rows: 1

SELECT LN(0);
> exception INVALID_VALUE_2

SELECT LN(-1);
> exception INVALID_VALUE_2

SELECT LOG(0, 2);
> exception INVALID_VALUE_2

SELECT LOG(-1, 2);
> exception INVALID_VALUE_2

SELECT LOG(1, 2);
> exception INVALID_VALUE_2

SELECT LOG(2, 0);
> exception INVALID_VALUE_2

SELECT LOG(2, -1);
> exception INVALID_VALUE_2

SELECT LOG(0);
> exception INVALID_VALUE_2

SELECT LOG(-1);
> exception INVALID_VALUE_2

SELECT LOG10(0);
> exception INVALID_VALUE_2

SELECT LOG10(-1);
> exception INVALID_VALUE_2

SELECT LN(0.5) VH, LN(1) V1, LN(2) V2, LN(3) V3, LN(10) V10;
> VH                  V1  V2                 V3                 V10
> ------------------- --- ------------------ ------------------ -----------------
> -0.6931471805599453 0.0 0.6931471805599453 1.0986122886681098 2.302585092994046
> rows: 1

SELECT LOG(2, 0.5) VH, LOG(2, 1) V1, LOG(2, 2) V2, LOG(2, 3) V3, LOG(2, 10) V10, LOG(2, 64) V64;
> VH   V1  V2  V3                 V10                V64
> ---- --- --- ------------------ ------------------ ---
> -1.0 0.0 1.0 1.5849625007211563 3.3219280948873626 6.0
> rows: 1

SELECT LOG(2.7182818284590452, 10);
>> 2.302585092994046

SELECT LOG(10, 3);
>> 0.47712125471966244

SELECT LOG(0.5) VH, LOG(1) V1, LOG(2) V2, LOG(3) V3, LOG(10) V10;
> VH                  V1  V2                 V3                 V10
> ------------------- --- ------------------ ------------------ -----------------
> -0.6931471805599453 0.0 0.6931471805599453 1.0986122886681098 2.302585092994046
> rows: 1

SELECT LOG10(0.5) VH, LOG10(1) V1, LOG10(2) V2, LOG10(3) V3, LOG10(10) V10, LOG10(100) V100;
> VH                  V1  V2                 V3                  V10 V100
> ------------------- --- ------------------ ------------------- --- ----
> -0.3010299956639812 0.0 0.3010299956639812 0.47712125471966244 1.0 2.0
> rows: 1

SET MODE PostgreSQL;
> ok

SELECT LOG(0.5) VH, LOG(1) V1, LOG(2) V2, LOG(3) V3, LOG(10) V10, LOG(100) V100;
> VH                  V1  V2                 V3                  V10 V100
> ------------------- --- ------------------ ------------------- --- ----
> -0.3010299956639812 0.0 0.3010299956639812 0.47712125471966244 1.0 2.0
> rows: 1

SET MODE MSSQLServer;
> ok

SELECT LOG(0.5, 2) VH, LOG(1, 2) V1, LOG(2, 2) V2, LOG(3, 2) V3, LOG(10, 2) V10, LOG(64, 2) V64;
> VH   V1  V2  V3                 V10                V64
> ---- --- --- ------------------ ------------------ ---
> -1.0 0.0 1.0 1.5849625007211563 3.3219280948873626 6.0
> rows: 1

SET MODE Regular;
> ok
