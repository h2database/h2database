create memory table sequence (id INT NOT NULL AUTO_INCREMENT, title varchar(255));
> ok

INSERT INTO sequence (title) VALUES ('test');
> update count: 1

INSERT INTO sequence (title) VALUES ('test1');
> update count: 1

--UPDATE sequence SET id=LAST_INSERT_ID(id+1);
--> update count: 1

SELECT LAST_INSERT_ID() AS L;
> L
> -
> 2
> rows: 1


SELECT LAST_INSERT_ID(100) AS L;
> L
> ---
> 100
> rows: 1


SELECT LAST_INSERT_ID() AS L;
> L
> ---
> 100
> rows: 1

INSERT INTO sequence (title) VALUES ('test2');
> update count: 1


SELECT MAX(id) AS M FROM sequence;
> M
> -
> 3
> rows: 1

SELECT LAST_INSERT_ID() AS L;
> L
> -
> 3
> rows: 1


DROP TABLE sequence;
> ok
