-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT STRINGDECODE('\7');
> exception STRING_FORMAT_ERROR_1

SELECT STRINGDECODE('\17');
> exception STRING_FORMAT_ERROR_1

SELECT STRINGDECODE('\117');
>> O

SELECT STRINGDECODE('\178');
> exception STRING_FORMAT_ERROR_1

SELECT STRINGDECODE('\u111');
> exception STRING_FORMAT_ERROR_1

SELECT STRINGDECODE('\u0057');
>> W
