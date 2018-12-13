-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(I NUMERIC(-1));
> exception INVALID_VALUE_2

CREATE TABLE TEST(I NUMERIC(-1, -1));
> exception INVALID_VALUE_2
