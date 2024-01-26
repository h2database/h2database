-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--
SET MODE MySQL;
> ok

create memory table sequence (id INT NOT NULL AUTO_INCREMENT, title varchar(255));
> ok

INSERT INTO sequence (title) VALUES ('test');
> update count: 1

INSERT INTO sequence (title) VALUES ('test1');
> update count: 1

SELECT LAST_INSERT_ID() AS L;
>> 2

SELECT LAST_INSERT_ID(100) AS L;
>> 100

SELECT LAST_INSERT_ID() AS L;
>> 100

INSERT INTO sequence (title) VALUES ('test2');
> update count: 1

SELECT MAX(id) AS M FROM sequence;
>> 3

SELECT LAST_INSERT_ID() AS L;
>> 3

SELECT LAST_INSERT_ID(NULL) AS L;
>> null

SELECT LAST_INSERT_ID() AS L;
>> 0


DROP TABLE sequence;
> ok
