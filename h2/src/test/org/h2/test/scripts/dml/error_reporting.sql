-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT 0x;
> exception SYNTAX_ERROR_2

SELECT 0xZ;
> exception SYNTAX_ERROR_2

SELECT 0xAAZ;
> exception SYNTAX_ERROR_2

SELECT 0x1LZ;
> exception SYNTAX_ERROR_2

SELECT 0x1234567890abZ;
> exception SYNTAX_ERROR_2

SELECT 0x1234567890abLZ;
> exception SYNTAX_ERROR_2

CREATE TABLE test (id INT NOT NULL, name VARCHAR);
> ok

select * from test where id = ARRAY [1, 2];
> exception TYPES_ARE_NOT_COMPARABLE_2

insert into test values (1, 't');
> update count: 1

select * from test where id = (1, 2);
> exception TYPES_ARE_NOT_COMPARABLE_2

drop table test;
> ok

SELECT 1 + 2 NOT;
> exception SYNTAX_ERROR_2

SELECT 1 NOT > 2;
> exception SYNTAX_ERROR_2
