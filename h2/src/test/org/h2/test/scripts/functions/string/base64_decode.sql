-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CALL base64_decode(null);
>> null

CALL '>' || utf8tostring(base64_decode('')) || '<';
>> ><

CALL utf8tostring(base64_decode('QQ=='));
>> A

CALL utf8tostring(base64_decode('QUI='));
>> AB

CALL utf8tostring(base64_decode('QUJD'));
>> ABC

CALL utf8tostring(base64_decode('QUJDRA=='));
>> ABCD

CALL utf8tostring(base64_decode('bXkgc3RyaW5n'));
>> my string

CALL utf8tostring(base64_decode('PDw/Pz4+'));
>> <<??>>

CALL utf8tostring(base64_decode('PDw_Pz4-'));
>> <<??>>

CALL base64_decode('the text should lead to an error');
> exception INVALID_VALUE_2
