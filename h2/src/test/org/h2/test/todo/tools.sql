/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

-- update all rows in all tables
select 'update ' || table_schema || '.' || table_name || ' set ' || column_name || '=' || column_name || ';'
from information_schema.columns where ORDINAL_POSITION = 1 and table_schema <> 'INFORMATION_SCHEMA';

-- read the first few bytes from a BLOB
drop table test;
drop alias first_bytes;
create alias first_bytes as $$
import java.io.*;
@CODE
byte[] firstBytes(InputStream in, int len) throws IOException {
    try {
        byte[] data = new byte[len];
        DataInputStream din = new DataInputStream(in);
        din.readFully(data, 0, len);
        return data;
    } finally {
        in.close();
    }
}
$$;
create table test(data blob);
insert into test values('010203040506070809');
select first_bytes(data, 3) from test;
