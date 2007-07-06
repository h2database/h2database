drop schema if exists pg_catalog;
create schema pg_catalog;

create view pg_catalog.pg_type(oid, typlen, typbasetype, typname, typnamespace) as 
select 
	data_type oid, 
	precision,
	data_type, 
	cast(type_name as varchar_ignorecase) typbasetype, 
	select id from information_schema.schemata where schema_name='PG_CATALOG'
from information_schema.type_info
union select
	1111,
	64,
	1111,
	'name',
	select id from information_schema.schemata where schema_name='PG_CATALOG'
from dual;
	
create view pg_catalog.pg_namespace(oid, nspname) as 
select id, cast(schema_name as varchar_ignorecase) 
from information_schema.schemata;

create view pg_catalog.pg_class(oid, relname, relnamespace, relkind) as 
select 
	id, 
	cast(table_name as varchar_ignorecase), 
	(select id from information_schema.schemata where schema_name = table_schema), 
	case table_type when 'TABLE' then 'r' else 'v' end 
from information_schema.tables;

create view pg_catalog.pg_description(objoid, objsubid, classoid, description) as 
select id, 0, -1, cast('' as varchar_ignorecase) 
from information_schema.tables where 1=0;

create view pg_catalog.pg_attrdef(oid, adsrc, adrelid, adnum) as 
select id, 0, 0, 0
from information_schema.tables where 1=0;

create view pg_catalog.pg_attribute(oid, attname, atttypid, attnotnull, atttypmod, 
attlen, attnum, attrelid, attisdropped) as 
select
t.id*10000 + ordinal_position, column_name, data_type, false, -1,
numeric_precision, ordinal_position, t.id, false
from
information_schema.tables t,
information_schema.columns c
where t.table_name = c.table_name
and t.table_schema = c.table_schema;

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

