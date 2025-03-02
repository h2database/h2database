SET MODE PostgreSQL;
> ok

CREATE TABLE users (firstname text, lastname text, id serial primary key);
> ok

INSERT INTO users (firstname, lastname) VALUES ('Joe', 'Cool');
> update count: 1

UPDATE users u SET u.firstname = 'Jack' RETURNING u.id, u.firstname;
> ID FIRSTNAME
> -- ---------
> 1  Jack
> rows: 1

INSERT INTO users (firstname, lastname) VALUES ('Joe', 'Cool') RETURNING id, firstname, lastname;
> ID FIRSTNAME LASTNAME
> -- --------- --------
> 2  Joe       Cool
> rows: 1

INSERT INTO users (firstname, lastname) VALUES ('Joe', 'Cool'), ('Hans', 'Grüber') RETURNING *;
> FIRSTNAME LASTNAME ID
> --------- -------- --
> Hans      Grüber   4
> Joe       Cool     3
> rows: 2

DELETE FROM users u WHERE u.firstname = 'Hans' RETURNING *;
> FIRSTNAME LASTNAME ID
> --------- -------- --
> Hans      Grüber   4
> rows: 1

CREATE TABLE DESTINATION(ID INT PRIMARY KEY, "VALUE" INT);
> ok

CREATE TABLE SOURCE(ID INT PRIMARY KEY, "VALUE" INT);
> ok

INSERT INTO SOURCE VALUES (1, 10), (3, 30), (5, 50);
> update count: 3

INSERT INTO DESTINATION VALUES (3, 300), (6, 600);
> update count: 2

MERGE INTO DESTINATION d USING SOURCE s ON (d.ID = s.ID)
    WHEN MATCHED THEN UPDATE SET d."VALUE" = s."VALUE"
    WHEN NOT MATCHED THEN INSERT (ID, "VALUE") VALUES (s.ID, s."VALUE")
    RETURNING d.*, s.ID;
> ID VALUE ID
> -- ----- --
> 1  10    1
> 3  30    3
> 5  50    5
> rows: 3

-- INIT database
CREATE TABLE Product (Id BIGINT PRIMARY KEY, Name VARCHAR(100), Description VARCHAR(255), N BIGINT);
> ok

CREATE TABLE Products (Id BIGINT PRIMARY KEY);
> ok

INSERT INTO Products(Id) VALUES (1), (2), (4);
> update count: 3

INSERT INTO Product(Id, Name, Description) VALUES (1, 'Entity Framework Extensions', 'Extend your DbContext with high-performance bulk operations.');
> update count: 1

INSERT INTO Product(Id, Name, Description) VALUES (2, 'Dapper Plus', 'Extend your IDbConnection with high-performance bulk operations.');
> update count: 1

INSERT INTO Product(Id, Name, Description) VALUES (3, 'C# Eval Expression', 'Compile and execute C# code at runtime.');
> update count: 1

WITH ATJ AS (
    MERGE INTO Product p USING (SELECT p1.Id AS Pid, p2.* FROM Products p1 LEFT OUTER JOIN Product p2 ON p1.Id = p2.Id) d ON p.Id = d.Id
    WHEN MATCHED AND p.Id = 1 THEN DELETE
    WHEN MATCHED AND p.Id = 2 THEN UPDATE SET Name = p.Id || ': ' || p.Name
-- TODO:    WHEN NOT MATCHED BY SOURCE THEN UPDATE SET Description = p.Id || ': ' || p.Description
    WHEN NOT MATCHED BY TARGET THEN INSERT (Id, Name, Description) VALUES (d.Pid, 'Kaas', 'Desc')
    RETURNING merge_action(), p.*, d.Id * 10 AS IDKEERTIEN
)
SELECT *
FROM ATJ
ORDER BY IDKEERTIEN DESC;
> MERGE_ACTION ID NAME                        DESCRIPTION                                                      N    IDKEERTIEN
> ------------ -- --------------------------- ---------------------------------------------------------------- ---- ----------
> UPDATE       2  2: Dapper Plus              Extend your IDbConnection with high-performance bulk operations. null 20
> DELETE       1  Entity Framework Extensions Extend your DbContext with high-performance bulk operations.     null 10
> INSERT       4  Kaas                        Desc                                                             null null
> rows (ordered): 3

-- MERGE_ACTION NOT ALLOWED
INSERT INTO Product(Id, Name, Description) VALUES (100, 'Fake name', 'Unknown description')
RETURNING MERGE_ACTION();
> exception SYNTAX_ERROR_2

