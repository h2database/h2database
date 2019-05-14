-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE testTranslate(id BIGINT, txt1 VARCHAR);
> ok

INSERT INTO testTranslate(id, txt1) values(1, 'test1'), (2, NULL), (3, ''), (4, 'caps');
> update count: 4

SELECT TRANSLATE(txt1, 'p', 'r') FROM testTranslate ORDER BY id;
> TRANSLATE(TXT1, 'p', 'r')
> -------------------------
> test1
> null
>
> cars
> rows (ordered): 4

SELECT TRANSLATE(NULL, NULL, NULL);
>> null

DROP TABLE testTranslate;
> ok
