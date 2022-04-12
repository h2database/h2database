-- Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select bit_length(null) en, bit_length('') e0, bit_length('ab') e32;
> EN   E0 E32
> ---- -- ---
> null 0  16
> rows: 1

select length(null) en, length('') e0, length('ab') e2;
> EN   E0 E2
> ---- -- --
> null 0  2
> rows: 1

select char_length(null) en, char_length('') e0, char_length('ab') e2;
> EN   E0 E2
> ---- -- --
> null 0  2
> rows: 1

select character_length(null) en, character_length('') e0, character_length('ab') e2;
> EN   E0 E2
> ---- -- --
> null 0  2
> rows: 1

select octet_length(null) en, octet_length('') e0, octet_length('ab') e4;
> EN   E0 E4
> ---- -- --
> null 0  2
> rows: 1
