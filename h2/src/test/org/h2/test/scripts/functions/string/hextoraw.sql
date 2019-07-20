-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select hextoraw(null) en, rawtohex(null) en1, hextoraw(rawtohex('abc')) abc;
> EN   EN1  ABC
> ---- ---- ---
> null null abc
> rows: 1

SELECT HEXTORAW('0049');
>> I

SET MODE Oracle;
> ok

SELECT HEXTORAW('0049');
>> 0049

SELECT HEXTORAW('0049') IS OF (RAW);
>> TRUE

SET MODE Regular;
> ok
