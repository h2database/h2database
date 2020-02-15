-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CALL '>' || base64_encode(stringtoutf8('')) || '<';
>> ><

CALL base64_encode(stringtoutf8('A'));
>> QQ==

CALL base64_encode(stringtoutf8('AB'));
>> QUI=

CALL base64_encode(stringtoutf8('ABC'));
>> QUJD

CALL base64_encode(stringtoutf8('ABCD'));
>> QUJDRA==

CALL base64_encode(stringtoutf8('my string'));
>> bXkgc3RyaW5n

CALL base64_encode(stringtoutf8('про'));
>> 0L/RgNC+

CALL base64_encode(stringtoutf8('про'), 'URL');
>> 0L_RgNC-