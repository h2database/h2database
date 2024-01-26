-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

VALUES '*' || TO_CHAR(CAST(-1 AS TINYINT), '999.99');
>> * -1.00

VALUES '*' || TO_CHAR(-11E-1, '999.99');
>> * -1.10
