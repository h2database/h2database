/*
 * Copyright 2004-2026 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
-- Copyright 2004-2026 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SET MAX_MEMORY_ROWS 100;
> ok

CREATE TABLE s(c0 INT, c1 INT);
> ok

INSERT INTO s SELECT x, MOD(x, 7) FROM SYSTEM_RANGE(0, 500);
> update count: 501

SELECT * FROM s ORDER BY c1, c0, c1 FETCH FIRST 3 ROWS ONLY;
> C0 C1
> -- --
> 0  0
> 7  0
> 14 0
> rows (ordered): 3

DROP TABLE s;
> ok

CREATE TABLE u(c0 INT);
> ok

INSERT INTO u SELECT x FROM SYSTEM_RANGE(0, 500);
> update count: 501

SET MAX_MEMORY_ROWS 100;
> ok

SELECT * FROM u ORDER BY c0, c0 FETCH FIRST 2 ROWS ONLY;
> C0
> --
> 0
> 1
> rows (ordered): 2

DROP TABLE u;
> ok
