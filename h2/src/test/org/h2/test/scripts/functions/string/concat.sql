create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');

select concat(null, null) en, concat(null, 'a') ea, concat('b', null) eb, concat('ab', 'c') abc from test;
> EN   EA EB ABC
> ---- -- -- ---
> null a  b  abc
> rows: 1

SELECT CONCAT('a', 'b', 'c', 'd') AS test;
> TEST
> ----
> abcd
> rows: 1


