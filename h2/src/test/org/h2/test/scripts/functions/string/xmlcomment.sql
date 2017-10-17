-- Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CALL XMLCOMMENT('Test');
> STRINGDECODE('<!-- Test -->\n')
> -------------------------------
> <!-- Test -->
> rows: 1

CALL XMLCOMMENT('--- test ---');
> STRINGDECODE('<!-- - - - test - - - -->\n')
> -------------------------------------------
> <!-- - - - test - - - -->
> rows: 1

