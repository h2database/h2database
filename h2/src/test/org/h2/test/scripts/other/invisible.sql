-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create table test(id int, name varchar invisible);
> ok

select * from test;
> ID
> --
> rows: 0

alter table test alter column name set visible;
> ok

select * from test;
> ID NAME
> -- ----
> rows: 0

alter table test add modify_date timestamp invisible before name;
> ok

select * from test;
> ID NAME
> -- ----
> rows: 0

alter table test alter column modify_date timestamp visible;
> ok

select * from test;
> ID MODIFY_DATE NAME
> -- ----------- ----
> rows: 0

alter table test alter column modify_date set invisible;
> ok

select * from test;
> ID NAME
> -- ----
> rows: 0

drop table test;
> ok

CREATE TABLE TEST(A INT, B INT INVISIBLE, C INT);
> ok

INSERT INTO TEST VALUES (1, 2);
> update count: 1

SELECT * FROM TEST;
> A C
> - -
> 1 2
> rows: 1

SELECT A, B, C FROM TEST;
> A B    C
> - ---- -
> 1 null 2
> rows: 1

ALTER TABLE TEST ADD D INT INVISIBLE;
> ok

ALTER TABLE TEST ADD E INT;
> ok

MERGE INTO TEST USING (VALUES (4, 5, 6)) T(A, C, E) ON TEST.A = T.A
WHEN NOT MATCHED THEN INSERT VALUES (T.A, T.C, T.E);
> update count: 1

SELECT * FROM TEST;
> A C E
> - - ----
> 1 2 null
> 4 5 6
> rows: 2

SELECT COLUMN_NAME, IS_VISIBLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME IS_VISIBLE
> ----------- ----------
> A           TRUE
> B           FALSE
> C           TRUE
> D           FALSE
> E           TRUE
> rows (ordered): 5

DROP TABLE TEST;
> ok

