-- Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--
-- Issue #4351: MySQL-style two-argument DATEDIFF(end, start) in MODE=MySQL / MariaDB

SET MODE MySQL;
> ok

SELECT DATEDIFF('2026-06-08', '2026-06-01');
>> 7

SELECT DATEDIFF(DATE '2026-06-08', DATE '2026-06-01');
>> 7

SELECT DATEDIFF(TIMESTAMP '2026-06-08 23:59:59', TIMESTAMP '2026-06-01 00:00:00');
>> 7

SELECT DATEDIFF('2026-06-01', '2026-06-08');
>> -7

SELECT DATEDIFF('2026-06-01', '2026-06-01');
>> 0

-- H2 three-argument form still works in MySQL mode
SELECT DATEDIFF(DAY, DATE '2026-06-01', DATE '2026-06-08');
>> 7

SELECT DATEDIFF('DAY', DATE '2026-06-01', DATE '2026-06-08');
>> 7

SELECT DATEDIFF(YEAR, TIMESTAMP '2003-12-01 10:20:30', TIMESTAMP '2004-01-01 10:00:00');
>> 1

-- TIMESTAMPDIFF stays three-argument (MySQL native signature)
SELECT TIMESTAMPDIFF(DAY, DATE '2026-06-01', DATE '2026-06-08');
>> 7

SET MODE MariaDB;
> ok

SELECT DATEDIFF('2026-06-08', '2026-06-01');
>> 7

SELECT DATEDIFF(DAY, DATE '2026-06-01', DATE '2026-06-08');
>> 7

SET MODE Regular;
> ok

-- Two-argument form is not accepted outside MySQL/MariaDB modes
SELECT DATEDIFF('2026-06-08', '2026-06-01');
> exception INVALID_VALUE_2

SELECT DATEDIFF(DAY, DATE '2026-06-01', DATE '2026-06-08');
>> 7
