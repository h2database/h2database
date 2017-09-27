create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select ifnull(null, '1') x1, ifnull(null, null) xn, ifnull('a', 'b') xa from test;
> X1 XN   XA
> -- ---- --
> 1  null a
> rows: 1

select isnull(null, '1') x1, isnull(null, null) xn, isnull('a', 'b') xa from test;
> X1 XN   XA
> -- ---- --
> 1  null a
> rows: 1

