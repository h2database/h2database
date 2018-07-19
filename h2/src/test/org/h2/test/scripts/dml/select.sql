-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(A INT, B INT, C INT);
> ok

INSERT INTO TEST VALUES (1, 1, 1), (1, 1, 2), (1, 1, 3), (1, 2, 1), (1, 2, 2), (1, 2, 3),
    (2, 1, 1), (2, 1, 2), (2, 1, 3), (2, 2, 1), (2, 2, 2), (2, 2, 3);
> update count: 12

SELECT * FROM TEST ORDER BY A, B;
> A B C
> - - -
> 1 1 1
> 1 1 2
> 1 1 3
> 1 2 1
> 1 2 2
> 1 2 3
> 2 1 1
> 2 1 2
> 2 1 3
> 2 2 1
> 2 2 2
> 2 2 3
> rows (ordered): 12

SELECT * FROM TEST ORDER BY A, B, C FETCH FIRST 4 ROWS ONLY;
> A B C
> - - -
> 1 1 1
> 1 1 2
> 1 1 3
> 1 2 1
> rows (ordered): 4

SELECT * FROM TEST ORDER  BY A, B, C FETCH FIRST 4 ROWS WITH TIES;
> A B C
> - - -
> 1 1 1
> 1 1 2
> 1 1 3
> 1 2 1
> rows: 4

SELECT * FROM TEST ORDER  BY A, B FETCH FIRST 4 ROWS WITH TIES;
> A B C
> - - -
> 1 1 1
> 1 1 2
> 1 1 3
> 1 2 1
> 1 2 2
> 1 2 3
> rows: 6

SELECT * FROM TEST ORDER  BY A FETCH FIRST 1 ROW WITH TIES;
> A B C
> - - -
> 1 1 1
> 1 1 2
> 1 1 3
> 1 2 1
> 1 2 2
> 1 2 3
> rows: 6

SELECT TOP (1) WITH TIES * FROM TEST ORDER  BY A;
> A B C
> - - -
> 1 1 1
> 1 1 2
> 1 1 3
> 1 2 1
> 1 2 2
> 1 2 3
> rows: 6

SELECT * FROM TEST ORDER  BY A, B OFFSET 3 ROWS FETCH NEXT 1 ROW WITH TIES;
> A B C
> - - -
> 1 2 1
> 1 2 2
> 1 2 3
> rows: 3

CREATE INDEX TEST_A_IDX ON TEST(A);
> ok

CREATE INDEX TEST_A_B_IDX ON TEST(A, B);
> ok

SELECT * FROM TEST ORDER  BY A FETCH FIRST 1 ROW WITH TIES;
> A B C
> - - -
> 1 1 1
> 1 1 2
> 1 1 3
> 1 2 1
> 1 2 2
> 1 2 3
> rows: 6

SELECT * FROM TEST ORDER  BY A, B OFFSET 3 ROWS FETCH NEXT 1 ROW WITH TIES;
> A B C
> - - -
> 1 2 1
> 1 2 2
> 1 2 3
> rows: 3

SELECT * FROM TEST FETCH FIRST 1 ROW WITH TIES;
> exception WITH_TIES_WITHOUT_ORDER_BY

(SELECT * FROM TEST) UNION (SELECT 1, 2, 4) ORDER  BY A, B OFFSET 3 ROWS FETCH NEXT 1 ROW WITH TIES;
> A B C
> - - -
> 1 2 1
> 1 2 2
> 1 2 3
> 1 2 4
> rows: 4

(SELECT * FROM TEST) UNION (SELECT 1, 2, 4) FETCH NEXT 1 ROW WITH TIES;
> exception WITH_TIES_WITHOUT_ORDER_BY

EXPLAIN SELECT * FROM TEST ORDER BY A, B OFFSET 3 ROWS FETCH NEXT 1 ROW WITH TIES;
>> SELECT TEST.A, TEST.B, TEST.C FROM PUBLIC.TEST /* PUBLIC.TEST_A_B_IDX */ ORDER BY 1, 2 OFFSET 3 ROWS FETCH NEXT 1 ROWS WITH TIES /* index sorted */

DROP TABLE TEST;
> ok

CREATE TABLE TEST(A NUMERIC, B NUMERIC);
> ok

INSERT INTO TEST VALUES (0, 1), (0.0, 2), (0, 3), (1, 4);
> update count: 4

SELECT A, B FROM TEST ORDER  BY A FETCH FIRST 1 ROW WITH TIES;
> A   B
> --- -
> 0   1
> 0   3
> 0.0 2
> rows: 3

DROP TABLE TEST;
> ok
