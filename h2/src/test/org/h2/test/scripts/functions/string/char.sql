create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');

select char(null) en, char(65) ea from test;
> EN   EA
> ---- --
> null A
> rows: 1

