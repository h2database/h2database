-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: Alex Nordlund
--

-- with filter condition

create table test(v varchar);
> ok

insert into test values ('1'), ('2'), ('3'), ('4'), ('5'), ('6'), ('7'), ('8'), ('9');
> update count: 9

select array_agg(v order by v asc),
    array_agg(v order by v desc) filter (where v >= '4')
    from test where v >= '2';
> ARRAY_AGG(V ORDER BY V)  ARRAY_AGG(V ORDER BY V DESC) FILTER (WHERE (V >= '4'))
> ------------------------ ------------------------------------------------------
> (2, 3, 4, 5, 6, 7, 8, 9) (9, 8, 7, 6, 5, 4)
> rows (ordered): 1

create index test_idx on test(v);
> ok

select ARRAY_AGG(v order by v asc),
    ARRAY_AGG(v order by v desc) filter (where v >= '4')
    from test where v >= '2';
> ARRAY_AGG(V ORDER BY V)  ARRAY_AGG(V ORDER BY V DESC) FILTER (WHERE (V >= '4'))
> ------------------------ ------------------------------------------------------
> (2, 3, 4, 5, 6, 7, 8, 9) (9, 8, 7, 6, 5, 4)
> rows (ordered): 1

select ARRAY_AGG(v order by v asc),
    ARRAY_AGG(v order by v desc) filter (where v >= '4')
    from test;
> ARRAY_AGG(V ORDER BY V)     ARRAY_AGG(V ORDER BY V DESC) FILTER (WHERE (V >= '4'))
> --------------------------- ------------------------------------------------------
> (1, 2, 3, 4, 5, 6, 7, 8, 9) (9, 8, 7, 6, 5, 4)
> rows (ordered): 1

drop table test;
> ok

create table test (id int auto_increment primary key, v int);
> ok

insert into test(v) values (7), (2), (8), (3), (7), (3), (9), (-1);
> update count: 8

select array_agg(v) from test;
> ARRAY_AGG(V)
> -------------------------
> (7, 2, 8, 3, 7, 3, 9, -1)
> rows: 1

select array_agg(distinct v) from test;
> ARRAY_AGG(DISTINCT V)
> ---------------------
> (-1, 2, 3, 7, 8, 9)
> rows: 1

select array_agg(distinct v order by v desc) from test;
> ARRAY_AGG(DISTINCT V ORDER BY V DESC)
> -------------------------------------
> (9, 8, 7, 3, 2, -1)
> rows (ordered): 1

drop table test;
> ok

CREATE TABLE TEST (ID INT PRIMARY KEY, NAME VARCHAR);
> ok

INSERT INTO TEST VALUES (1, 'a'), (2, 'a'), (3, 'b'), (4, 'c'), (5, 'c'), (6, 'c');
> update count: 6

SELECT ARRAY_AGG(ID), NAME FROM TEST;
> exception MUST_GROUP_BY_COLUMN_1

SELECT ARRAY_AGG(ID), NAME FROM TEST GROUP BY NAME;
> ARRAY_AGG(ID) NAME
> ------------- ----
> (1, 2)        a
> (3)           b
> (4, 5, 6)     c
> rows: 3

SELECT ARRAY_AGG(ID) OVER (), NAME FROM TEST;
> ARRAY_AGG(ID) OVER () NAME
> --------------------- ----
> (1, 2, 3, 4, 5, 6)    a
> (1, 2, 3, 4, 5, 6)    a
> (1, 2, 3, 4, 5, 6)    b
> (1, 2, 3, 4, 5, 6)    c
> (1, 2, 3, 4, 5, 6)    c
> (1, 2, 3, 4, 5, 6)    c
> rows: 6

SELECT ARRAY_AGG(ID) OVER (PARTITION BY NAME), NAME FROM TEST;
> ARRAY_AGG(ID) OVER (PARTITION BY NAME) NAME
> -------------------------------------- ----
> (1, 2)                                 a
> (1, 2)                                 a
> (3)                                    b
> (4, 5, 6)                              c
> (4, 5, 6)                              c
> (4, 5, 6)                              c
> rows: 6

SELECT ARRAY_AGG(ID) FILTER (WHERE ID < 3 OR ID > 4) OVER (PARTITION BY NAME), NAME FROM TEST ORDER BY NAME;
> ARRAY_AGG(ID) FILTER (WHERE ((ID < 3) OR (ID > 4))) OVER (PARTITION BY NAME) NAME
> ---------------------------------------------------------------------------- ----
> (1, 2)                                                                       a
> (1, 2)                                                                       a
> null                                                                         b
> (5, 6)                                                                       c
> (5, 6)                                                                       c
> (5, 6)                                                                       c
> rows (ordered): 6

SELECT ARRAY_AGG(SUM(ID)) OVER () FROM TEST;
> ARRAY_AGG(SUM(ID)) OVER ()
> --------------------------
> (21)
> rows: 1

SELECT ARRAY_AGG(ID) OVER() FROM TEST GROUP BY ID;
> ARRAY_AGG(ID) OVER ()
> ---------------------
> (1, 2, 3, 4, 5, 6)
> (1, 2, 3, 4, 5, 6)
> (1, 2, 3, 4, 5, 6)
> (1, 2, 3, 4, 5, 6)
> (1, 2, 3, 4, 5, 6)
> (1, 2, 3, 4, 5, 6)
> rows: 6

SELECT ARRAY_AGG(NAME) OVER(PARTITION BY NAME) FROM TEST GROUP BY NAME;
> ARRAY_AGG(NAME) OVER (PARTITION BY NAME)
> ----------------------------------------
> (a)
> (b)
> (c)
> rows: 3

SELECT ARRAY_AGG(ARRAY_AGG(ID)) OVER (PARTITION BY NAME), NAME FROM TEST GROUP BY NAME;
> ARRAY_AGG(ARRAY_AGG(ID)) OVER (PARTITION BY NAME) NAME
> ------------------------------------------------- ----
> ((1, 2))                                          a
> ((3))                                             b
> ((4, 5, 6))                                       c
> rows: 3

SELECT ARRAY_AGG(ARRAY_AGG(ID)) OVER (PARTITION BY NAME), NAME FROM TEST GROUP BY NAME OFFSET 1 ROW;
> ARRAY_AGG(ARRAY_AGG(ID)) OVER (PARTITION BY NAME) NAME
> ------------------------------------------------- ----
> ((3))                                             b
> ((4, 5, 6))                                       c
> rows: 2

DROP TABLE TEST;
> ok
