-- Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select soundex(null) en, soundex('tom') et;
> EN   ET
> ---- ----
> null t500
> rows: 1

select
soundex('Washington') W252, soundex('Lee') L000,
soundex('Gutierrez') G362, soundex('Pfister') P236,
soundex('Jackson') J250, soundex('Tymczak') T522,
soundex('VanDeusen') V532, soundex('Ashcraft') A261;
> W252 L000 G362 P236 J250 T522 V532 A261
> ---- ---- ---- ---- ---- ---- ---- ----
> W252 L000 G362 P236 J250 T522 V532 A261
> rows: 1
