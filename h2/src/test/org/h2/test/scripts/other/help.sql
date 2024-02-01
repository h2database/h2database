-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

help abc;
> SECTION TOPIC SYNTAX TEXT
> ------- ----- ------ ----
> rows: 0

HELP ABCDE EF_GH;
> SECTION TOPIC SYNTAX TEXT
> ------- ----- ------ ----
> rows: 0

HELP HELP;
> SECTION          TOPIC SYNTAX                  TEXT
> ---------------- ----- ----------------------- ----------------------------------------------------
> Commands (Other) HELP  HELP [ anything [...] ] Displays the help pages of SQL commands or keywords.
> rows: 1

HELP he lp;
> SECTION          TOPIC SYNTAX                  TEXT
> ---------------- ----- ----------------------- ----------------------------------------------------
> Commands (Other) HELP  HELP [ anything [...] ] Displays the help pages of SQL commands or keywords.
> rows: 1

HELP 1;
> exception SYNTAX_ERROR_2
