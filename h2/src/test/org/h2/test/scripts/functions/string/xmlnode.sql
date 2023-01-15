-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CALL XMLNODE('a', XMLATTR('href', 'https://h2database.com'));
>> <a href="https://h2database.com"/>

CALL XMLNODE('br');
>> <br/>

CALL XMLNODE('p', null, 'Hello World');
>> <p>Hello World</p>

SELECT XMLNODE('p', null, 'Hello' || chr(10) || 'World');
>> <p> Hello World </p>

SELECT XMLNODE('p', null, 'Hello' || chr(10) || 'World', false);
>> <p>Hello World</p>
