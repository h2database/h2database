-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select casewhen(null, '1', '2') xn, casewhen(1>0, 'n', 'y') xy, casewhen(0<1, 'a', 'b') xa;
> XN XY XA
> -- -- --
> 2  n  a
> rows: 1
