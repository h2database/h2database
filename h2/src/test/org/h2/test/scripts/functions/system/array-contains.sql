-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select array_contains((4.0, 2.0, 2.0), 2.0);
>> TRUE

select array_contains((4.0, 2.0, 2.0), 5.0);
>> FALSE

select array_contains(('one', 'two'), 'one');
>> TRUE

select array_contains(('one', 'two'), 'xxx');
>> FALSE

select array_contains(('one', 'two'), null);
>> FALSE

select array_contains((null, 'two'), null);
>> TRUE

select array_contains(null, 'one');
>> null

select array_contains(((1, 2), (3, 4)), (1, 2));
>> TRUE

select array_contains(((1, 2), (3, 4)), (5, 6));
>> FALSE

CREATE TABLE TEST (ID INT PRIMARY KEY AUTO_INCREMENT, A ARRAY);
> ok

INSERT INTO TEST (A) VALUES ((1L, 2L)), ((3L, 4L));
> update count: 2

SELECT ID, ARRAY_CONTAINS(A, 1L), ARRAY_CONTAINS(A, 2L), ARRAY_CONTAINS(A, 3L), ARRAY_CONTAINS(A, 4L) FROM TEST;
> ID ARRAY_CONTAINS(A, 1) ARRAY_CONTAINS(A, 2) ARRAY_CONTAINS(A, 3) ARRAY_CONTAINS(A, 4)
> -- -------------------- -------------------- -------------------- --------------------
> 1  TRUE                 TRUE                 FALSE                FALSE
> 2  FALSE                FALSE                TRUE                 TRUE
> rows: 2

SELECT * FROM (
    SELECT ID, ARRAY_CONTAINS(A, 1L), ARRAY_CONTAINS(A, 2L), ARRAY_CONTAINS(A, 3L), ARRAY_CONTAINS(A, 4L) FROM TEST
);
> ID ARRAY_CONTAINS(A, 1) ARRAY_CONTAINS(A, 2) ARRAY_CONTAINS(A, 3) ARRAY_CONTAINS(A, 4)
> -- -------------------- -------------------- -------------------- --------------------
> 1  TRUE                 TRUE                 FALSE                FALSE
> 2  FALSE                FALSE                TRUE                 TRUE
> rows: 2

DROP TABLE TEST;
> ok
