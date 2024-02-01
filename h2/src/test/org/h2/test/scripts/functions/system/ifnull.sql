-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select ifnull(null, '1') x1, ifnull(null, null) xn, ifnull('a', 'b') xa;
> X1 XN   XA
> -- ---- --
> 1  null a
> rows: 1

SELECT ISNULL(NULL, '1');
> exception FUNCTION_NOT_FOUND_1

SET MODE MSSQLServer;
> ok

select isnull(null, '1') x1, isnull(null, null) xn, isnull('a', 'b') xa;
> X1 XN   XA
> -- ---- --
> 1  null a
> rows: 1

SET MODE Regular;
> ok

CREATE MEMORY TABLE S(D DOUBLE) AS VALUES NULL;
> ok

CREATE MEMORY TABLE T AS SELECT IFNULL(D, D) FROM S;
> ok

SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'T';
>> DOUBLE PRECISION

DROP TABLE S, T;
> ok
