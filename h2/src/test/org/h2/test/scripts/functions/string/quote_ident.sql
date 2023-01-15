-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT QUOTE_IDENT(NULL);
>> null

SELECT QUOTE_IDENT('');
>> ""

SELECT QUOTE_IDENT('a');
>> "a"

SELECT QUOTE_IDENT('"a""A"');
>> """a""""A"""
