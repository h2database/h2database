-- Copyright 2004-2026 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

VALUES '*' || TO_CHAR(CAST(-1 AS TINYINT), '999.99');
>> * -1.00

VALUES '*' || TO_CHAR(-11E-1, '999.99');
>> * -1.10

VALUES '*' || TO_CHAR(42, 'S999');
>> * +42

VALUES '*' || TO_CHAR(-42, '999S');
>> * 42-

VALUES TO_CHAR(150000, 'S');
>> #

VALUES TO_CHAR(-1, 's');
>> #

VALUES TO_CHAR(1, 'SV');
>> #

VALUES TO_CHAR(12345, 'S999V');
>> ####

VALUES TO_CHAR(12345, 'S99999V');
>> +12345

VALUES TO_CHAR(-123, 'S999V99');
>> -12300

VALUES '*' || TO_CHAR(-12, 'S99V9MI');
>> *120-

VALUES '*' || TO_CHAR(-12, 's99v9pr');
>> *<120>
