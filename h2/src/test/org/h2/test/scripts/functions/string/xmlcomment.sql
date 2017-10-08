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

