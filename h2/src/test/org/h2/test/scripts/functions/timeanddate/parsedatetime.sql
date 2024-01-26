-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SET TIME ZONE '01:00';
> ok

CALL PARSEDATETIME('3. Februar 2001', 'd. MMMM yyyy', 'de');
>> 2001-02-03 00:00:00+01

CALL PARSEDATETIME('3. FEBRUAR 2001', 'd. MMMM yyyy', 'de');
>> 2001-02-03 00:00:00+01

CALL PARSEDATETIME('02/03/2001 04:05:06', 'MM/dd/yyyy HH:mm:ss');
>> 2001-02-03 04:05:06+01

CALL CAST(PARSEDATETIME('10:11:12', 'HH:mm:ss', 'en') AS TIME);
>> 10:11:12

CALL CAST(PARSEDATETIME('10:11:12', 'HH:mm:ss', 'en', 'GMT+2') AS TIME WITH TIME ZONE);
>> 10:11:12+02

SET TIME ZONE LOCAL;
> ok
