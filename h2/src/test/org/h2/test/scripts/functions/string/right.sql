create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select right(null, 10) en, right('abc', null) en2, right('boat-trip', 2) e_ip, right('', 1) ee, right('a', -1) ee2 from test;
> EN   EN2  E_IP EE EE2
> ---- ---- ---- -- ---
> null null ip
> rows: 1
