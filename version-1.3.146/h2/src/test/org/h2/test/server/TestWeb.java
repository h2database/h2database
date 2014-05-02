/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.server;

import org.h2.test.TestBase;
import org.h2.tools.Server;

/**
 * Tests the H2 Console application.
 */
public class TestWeb extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        Server server = Server.createWebServer("-webPort", "8182");
        server.start();
        String url = server.getURL();
        WebClient client = new WebClient();
        String result = client.get(url);
        client.readSessionId(result);
        client.get(url, "login.jsp");
        client.get(url, "stylesheet.css");
        client.get(url, "adminSave.do");
        result = client.get(url, "index.do?language=de");
        result = client.get(url, "login.jsp");
        assertContains(result, "Einstellung");
        result = client.get(url, "index.do?language=en");
        result = client.get(url, "login.jsp");
        assertTrue(result.indexOf("Einstellung") < 0);
        result = client.get(url, "test.do?driver=abc&url=jdbc:abc:mem:web&user=sa&password=sa&name=_test_");
        assertContains(result, "Exception");
        result = client.get(url, "test.do?driver=org.h2.Driver&url=jdbc:h2:mem:web&user=sa&password=sa&name=_test_");
        assertTrue(result.indexOf("Exception") < 0);
        result = client.get(url, "login.do?driver=org.h2.Driver&url=jdbc:h2:mem:web&user=sa&password=sa&name=_test_");
        result = client.get(url, "header.jsp");
        result = client.get(url, "tables.do");
        result = client.get(url, "query.jsp");
        result = client.get(url, "query.do?sql=select * from test");
        result = client.get(url, "query.do?sql=drop table test if exists");
        result = client.get(url, "query.do?sql=create table test(id int primary key, name varchar);insert into test values(1, 'Hello')");
        result = client.get(url, "query.do?sql=select * from test");
        assertContains(result, "Hello");
        result = client.get(url, "query.do?sql=@META select * from test");
        assertContains(result, "typeName");
        result = client.get(url, "query.do?sql=delete from test");
        result = client.get(url, "query.do?sql=@LOOP 1000 insert into test values(?, 'Hello ' || ?/*RND*/)");
        assertContains(result, "1000 * (Prepared)");
        result = client.get(url, "query.do?sql=select * from test");
        result = client.get(url, "query.do?sql=@HISTORY");
        result = client.get(url, "getHistory.do?id=4");
        assertContains(result, "select * from test");
        result = client.get(url, "autoCompleteList.do?query=se");

//        long time = System.currentTimeMillis();
//        for (int i=0; i<1000; i++) {
//            if(System.currentTimeMillis()-time > 15000) {
//                break;
//            }
//        result = client.get(url, "autoCompleteList.do?query=select * from ");

        assertContains(result, "select");
        assertContains(result, "set");
        result = client.get(url, "tables.do");
        assertContains(result, "TEST");
        result = client.get(url, "autoCompleteList.do?query=select * from ");
        assertContains(result, "test");
        result = client.get(url, "autoCompleteList.do?query=select * from test t where t.");
        assertContains(result, "id");
        result = client.get(url, "autoCompleteList.do?query=select id x from test te where t");
        assertContains(result, "te");

        result = client.get(url, "query.do?sql=delete from test");
        result = client.get(url, "query.do?sql=@LOOP 10 @STATEMENT insert into test values(?, 'Hello')");
        result = client.get(url, "query.do?sql=select * from test");
        assertContains(result, "8");
        result = client.get(url, "query.do?sql=@EDIT select * from test");
        assertContains(result, "editRow");

        result = client.get(url, "query.do?sql=@AUTOCOMMIT TRUE");
        result = client.get(url, "query.do?sql=@AUTOCOMMIT FALSE");
        result = client.get(url, "query.do?sql=@TRANSACTION_ISOLATION");
        result = client.get(url, "query.do?sql=@SET MAXROWS 1");
        result = client.get(url, "query.do?sql=select * from test order by id");
        result = client.get(url, "query.do?sql=@SET MAXROWS 1000");
        result = client.get(url, "query.do?sql=@TABLES");
        assertContains(result, "TEST");
        result = client.get(url, "query.do?sql=@COLUMNS null null TEST");
        assertContains(result, "ID");
        result = client.get(url, "query.do?sql=@INDEX_INFO null null TEST");
        assertContains(result, "PRIMARY");
        result = client.get(url, "query.do?sql=@CATALOG");
        assertContains(result, "PUBLIC");
        result = client.get(url, "query.do?sql=@MEMORY");
        assertContains(result, "Used");
        result = client.get(url, "query.do?sql=@UDTS");

        result = client.get(url, "query.do?sql=@INFO");
        assertContains(result, "getCatalog");

        result = client.get(url, "logout.do");
        result = client.get(url, "settingRemove.do?name=_test_");

        client.get(url, "admin.do");
        // this would also stop the server
        // client.get(url, "adminShutdown.do");

        server.stop();
    }

}
