-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT BIT_XOR(V), BIT_XOR(DISTINCT V), BIT_XOR(V) FILTER (WHERE V <> 1) FROM (VALUES 1, 1, 2, 3, 4) T(V);
> BIT_XOR(V) BIT_XOR(DISTINCT V) BIT_XOR(V) FILTER (WHERE (V <> 1))
> ---------- ------------------- ----------------------------------
> 5          4                   5
> rows: 1
