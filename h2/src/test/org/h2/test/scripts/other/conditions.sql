-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT
    NULL AND NULL, NULL AND FALSE, NULL AND TRUE,
    FALSE AND NULL, FALSE AND FALSE, FALSE AND TRUE,
    TRUE AND NULL, TRUE AND FALSE, TRUE AND TRUE;
> UNKNOWN FALSE UNKNOWN FALSE FALSE FALSE UNKNOWN FALSE TRUE
> ------- ----- ------- ----- ----- ----- ------- ----- ----
> null    FALSE null    FALSE FALSE FALSE null    FALSE TRUE
> rows: 1

SELECT
    NULL OR NULL, NULL OR FALSE, NULL OR TRUE,
    FALSE OR NULL, FALSE OR FALSE, FALSE OR TRUE,
    TRUE OR NULL, TRUE OR FALSE, TRUE OR TRUE;
> UNKNOWN UNKNOWN TRUE UNKNOWN FALSE TRUE TRUE TRUE TRUE
> ------- ------- ---- ------- ----- ---- ---- ---- ----
> null    null    TRUE null    FALSE TRUE TRUE TRUE TRUE
> rows: 1

SELECT NOT NULL, NOT FALSE, NOT TRUE;
> UNKNOWN TRUE FALSE
> ------- ---- -----
> null    TRUE FALSE
> rows: 1

SELECT 0 AND TRUE;
>> FALSE

SELECT TRUE AND 0;
>> FALSE

SELECT 1 OR FALSE;
>> TRUE

SELECT FALSE OR 1;
>> TRUE

SELECT NOT 0;
>> TRUE

SELECT NOT 1;
>> FALSE
