/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
;
drop schema if exists pg_catalog;
create schema pg_catalog;

set search_path = PUBLIC, pg_catalog;

create table pg_catalog.pg_namespace -- (oid, nspname) 
as 
select 
    id oid, 
    cast(schema_name as varchar_ignorecase) nspname
from information_schema.schemata;

create table pg_catalog.pg_type(
    oid int, 
    typname varchar_ignorecase, 
    typnamespace int, 
    typlen int, 
    typbasetype int);
    
insert into pg_catalog.pg_type
select
    data_type oid, 
    cast(type_name as varchar_ignorecase) typname,
    (select oid from pg_catalog.pg_namespace where nspname = 'pg_catalog') typnamespace,
    -1 typlen,
    0 typbasetype
from information_schema.type_info;

insert into pg_catalog.pg_type values(
    1111, 
    'name',
    (select oid from pg_catalog.pg_namespace where nspname = 'pg_catalog'),
    -1,
    0
);
    
create table pg_catalog.pg_class -- (oid, relname, relnamespace, relkind, relam, reltuples, relpages, relhasrules, relhasoids) 
as 
select 
    id oid, 
    cast(table_name as varchar_ignorecase) relname, 
    (select id from information_schema.schemata where schema_name = table_schema) relnamespace, 
    case table_type when 'TABLE' then 'r' else 'v' end relkind,
    0 relam,
    cast(0 as float) reltuples,
    0 relpages,
    false relhasrules,
    false relhasoids
from information_schema.tables
union all
select
    id oid,
    cast(index_name as varchar_ignorecase) relname,
    (select id from information_schema.schemata where schema_name = table_schema) relnamespace, 
    'i' relkind,
    0 relam,
    cast(0 as float) reltuples,
    0 relpages,
    false relhasrules,
    false relhasoids    
from information_schema.indexes;

create table pg_catalog.pg_description -- (objoid, objsubid, classoid, description) 
as 
select 
    id objoid, 
    0 objsubid, 
    -1 classoid, 
    cast('' as varchar_ignorecase) description
from information_schema.tables where 1=0;

create table pg_catalog.pg_proc(
    oid int,
    proname varchar_ignorecase
);
    
create table pg_catalog.pg_trigger(
    oid int,
    tgconstrrelid int,
    tgfoid int,
    tgargs int,
    tgnargs int,
    tgdeferrable boolean,
    tginitdeferred boolean,
    tgconstrname varchar_ignorecase,
    tgrelid int
);

create table pg_catalog.pg_attrdef -- (oid, adsrc, adrelid, adnum) 
as 
select 
    id oid, 
    0 adsrc, 
    0 adrelid, 
    0 adnum
from information_schema.tables where 1=0;

create table pg_catalog.pg_attribute -- (oid, attrelid, attname, atttypid, attlen, attnum, atttypmod, attnotnull, attisdropped, atthasdef) 
as 
select
    t.id*10000 + c.ordinal_position oid, 
    t.id attrelid,
     c.column_name attname,
     data_type atttypid,
     -1 attlen,
     c.ordinal_position attnum,
     -1 atttypmod,
     false attnotnull,
     false attisdropped,
     false atthasdef
from information_schema.tables t, information_schema.columns c
where t.table_name = c.table_name
and t.table_schema = c.table_schema
union all
select
    1000000 + t.id*10000 + c.ordinal_position oid, 
    i.id attrelid,
     c.column_name attname,
     data_type atttypid,
     -1 attlen,
     c.ordinal_position attnum,
     -1 atttypmod,
     false attnotnull,
     false attisdropped,
     false atthasdef
from information_schema.tables t, information_schema.indexes i, information_schema.columns c
where t.table_name = i.table_name
and t.table_schema = i.table_schema
and t.table_name = c.table_name
and t.table_schema = c.table_schema;

create table pg_catalog.pg_index -- (oid, indexrelid, indrelid, indisclustered, indisunique, indisprimary, indexprs, indkey) 
as 
select 
    i.id oid,
    i.id indexrelid,
    t.id indrelid,
    false indisclustered,
    not non_unique indisunique,
    primary_key indisprimary,
    cast(null as varchar_ignorecase) indexprs,
    cast(0 as array) indkey
from information_schema.indexes i, information_schema.tables t
where i.table_schema = t.table_schema 
and i.table_name = t.table_name
and i.ordinal_position = 1;

drop alias if exists pg_get_indexdef;
create alias pg_get_indexdef for "org.h2.server.pg.PgServer.getIndexColumn";

drop alias if exists version;
create alias version for "org.h2.server.pg.PgServer.getVersion";

drop alias if exists current_schema;
create alias current_schema for "org.h2.server.pg.PgServer.getCurrentSchema";

drop alias if exists pg_encoding_to_char;
create alias pg_encoding_to_char for "org.h2.server.pg.PgServer.getEncodingName";

drop alias if exists pg_postmaster_start_time;
create alias pg_postmaster_start_time for "org.h2.server.pg.PgServer.getStartTime";

drop alias if exists pg_get_userbyid;
create alias pg_get_userbyid for "org.h2.server.pg.PgServer.getUserById";

drop alias if exists has_database_privilege;
create alias has_database_privilege for "org.h2.server.pg.PgServer.hasDatabasePrivilege";

drop alias if exists has_table_privilege;
create alias has_table_privilege for "org.h2.server.pg.PgServer.hasTablePrivilege";

drop alias if exists currtid2;
create alias currtid2 for "org.h2.server.pg.PgServer.getCurrentTid";

create table pg_catalog.pg_database(
    oid int, 
    datname varchar_ignorecase, 
    encoding int,
    datlastsysoid int,
    datallowconn boolean,
    datconfig array, -- text[]
    datacl array, -- aclitem[]
    datdba int,
    dattablespace int
);

insert into pg_catalog.pg_database values(
    0, -- oid
    'postgres', -- datname
    6, -- encoding, UTF8
    100000, -- datlastsysoid
    true, -- datallowconn
    null, -- datconfig
    null, -- datacl
    select min(id) from information_schema.users where admin=true, -- datdba
    0 -- dattablespace
);

create table pg_catalog.pg_tablespace(
    oid int,
    spcname varchar_ignorecase,
    spclocation varchar_ignorecase,
    spcowner int,
    spcacl array -- aclitem[]
);

insert into pg_catalog.pg_tablespace values(
    0,
    'main', -- spcname
    '?', -- spclocation
    0, -- spcowner,
    null -- spcacl
);

create table pg_catalog.pg_settings(
    oid int, 
    name varchar_ignorecase, 
    setting varchar_ignorecase
);

insert into pg_catalog.pg_settings values
(0, 'autovacuum', 'on'),
(1, 'stats_start_collector', 'on'),
(2, 'stats_row_level', 'on');

create table pg_catalog.pg_user(
    oid int,
    usename varchar_ignorecase,
    usecreatedb boolean,
    usesuper boolean);

insert into pg_catalog.pg_user select
    id oid,
    cast(name as varchar_ignorecase) usename,
    true usecreatedb,
    true usesuper
from information_schema.users;

create table pg_catalog.pg_authid(
    oid int,
    rolname varchar_ignorecase,
    rolsuper boolean,
    rolinherit boolean,
    rolcreaterole boolean,
    rolcreatedb boolean,
    rolcatupdate boolean,
    rolcanlogin boolean,
    rolconnlimit boolean,
    rolpassword boolean,
    rolvaliduntil timestamp, -- timestamptz
    rolconfig array -- text[]
);

create table pg_catalog.pg_am(oid int, amname varchar_ignorecase);
insert into  pg_catalog.pg_am values(0, 'btree');
insert into  pg_catalog.pg_am values(1, 'hash');

SELECT 
    NULL AS TABLE_CAT, 
    n.nspname AS TABLE_SCHEM, 
    ct.relname AS TABLE_NAME, 
    NOT i.indisunique AS NON_UNIQUE, 
    NULL AS INDEX_QUALIFIER, 
    ci.relname AS INDEX_NAME,  
    CASE i.indisclustered  WHEN true THEN 1 ELSE CASE am.amname  WHEN 'hash' THEN 2 ELSE 3 END  END AS TYPE,  
    a.attnum AS ORDINAL_POSITION,  
    CASE i.indexprs 
    WHEN null THEN a.attname 
    ELSE pg_get_indexdef(ci.oid,a.attnum,false) END 
    AS COLUMN_NAME,  
    NULL AS ASC_OR_DESC,  
    ci.reltuples AS CARDINALITY,  
    ci.relpages AS PAGES,  
    NULL AS FILTER_CONDITION  
 FROM pg_catalog.pg_namespace n, 
 pg_catalog.pg_class ct, 
 pg_catalog.pg_class ci, 
 pg_catalog.pg_attribute a, 
 pg_catalog.pg_am am , 
 pg_catalog.pg_index i  
 WHERE ct.oid=i.indrelid 
 AND ci.oid=i.indexrelid 
 AND a.attrelid=ci.oid 
 AND ci.relam=am.oid  
 AND n.oid = ct.relnamespace  
 AND n.nspname = 'PUBLIC'  
 AND ct.relname = 'TEST'  
 ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION ;
 

