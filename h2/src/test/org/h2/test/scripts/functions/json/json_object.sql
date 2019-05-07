-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT JSON_OBJECT('key1' : 10, 'key2' VALUE TRUE, KEY 'key3' VALUE 'str', 'key4' : NULL, 'key5' : '[1,2,3]'::JSON);
>> {"key1":10,"key2":true,"key3":"str","key4":null,"key5":[1,2,3]}

SELECT JSON_OBJECT('key1' : NULL ABSENT ON NULL);
>> {}

SELECT JSON_OBJECT('key1' : NULL NULL ON NULL);
>> {"key1":null}

SELECT JSON_OBJECT('key1' : NULL, 'key1' : 2 NULL ON NULL WITHOUT UNIQUE KEYS);
>> {"key1":null,"key1":2}

SELECT JSON_OBJECT('key1' : 1, 'key1' : 2 WITH UNIQUE KEYS);
> exception INVALID_VALUE_2

SELECT JSON_OBJECT('key1' : 1, 'key1' : 2 NULL ON NULL WITH UNIQUE KEYS);
> exception INVALID_VALUE_2

SELECT JSON_OBJECT('key1' : TRUE WITH UNIQUE KEYS);
>> {"key1":true}

SELECT JSON_OBJECT('key' || NULL : 1);
> exception INVALID_VALUE_2
