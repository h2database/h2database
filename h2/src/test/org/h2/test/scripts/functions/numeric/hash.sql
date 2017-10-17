-- Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

call hash('SHA256', stringtoutf8('Hello'), 1);
> X'185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969'
> -------------------------------------------------------------------
> 185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969
> rows: 1

CALL HASH('SHA256', STRINGTOUTF8('Password'), 1000);
> X'c644a176ce920bde361ac336089b06cc2f1514dfa95ba5aabfe33f9a22d577f0'
> -------------------------------------------------------------------
> c644a176ce920bde361ac336089b06cc2f1514dfa95ba5aabfe33f9a22d577f0
> rows: 1
