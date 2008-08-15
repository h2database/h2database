-- update all rows in all tables
select 'update ' || table_schema || '.' || table_name || ' set ' || column_name || '=' || column_name || ';'
from information_schema.columns where ORDINAL_POSITION = 1 and table_schema <> 'INFORMATION_SCHEMA';
