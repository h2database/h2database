-- Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

@reconnect off

select rand(1) e, random() f;
> E                  F
> ------------------ -------------------
> 0.7308781907032909 0.41008081149220166
> rows: 1

select rand();
>> 0.20771484130971707
