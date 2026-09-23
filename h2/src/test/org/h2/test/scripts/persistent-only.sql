-- Copyright 2004-2026 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- #4380
CREATE MATERIALIZED VIEW mv AS SELECT X FROM SYSTEM_RANGE(1,3);
> ok

COMMENT ON TABLE mv IS 'comment';
> ok

@reconnect

SELECT * FROM mv;
> exception TABLE_OR_VIEW_NOT_FOUND_1
