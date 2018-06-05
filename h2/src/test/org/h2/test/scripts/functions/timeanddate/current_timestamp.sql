-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT CAST(CURRENT_TIMESTAMP AS TIMESTAMP) = LOCALTIMESTAMP;
>> TRUE

SELECT CAST(CURRENT_TIMESTAMP(0) AS TIMESTAMP) = LOCALTIMESTAMP(0);
>> TRUE
