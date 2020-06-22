-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select user() x_sa, current_user() x_sa2;
> X_SA X_SA2
> ---- -----
> SA   SA
> rows: 1

SELECT CURRENT_USER;
>> SA

SELECT SESSION_USER;
>> SA

SELECT SYSTEM_USER;
>> SA

SELECT CURRENT_ROLE;
>> PUBLIC

EXPLAIN SELECT CURRENT_USER, SESSION_USER, SYSTEM_USER, USER, CURRENT_ROLE;
>> SELECT CURRENT_USER, SESSION_USER, SYSTEM_USER, CURRENT_USER, CURRENT_ROLE
