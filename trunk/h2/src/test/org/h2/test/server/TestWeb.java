/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import org.h2.constant.SysProperties;
import org.h2.test.TestBase;
import org.h2.tools.Server;
import org.h2.store.fs.FileSystem;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;
import org.h2.util.Task;

/**
 * Tests the H2 Console application.
 */
public class TestWeb extends TestBase {

    private static volatile String lastUrl;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        testTools();
        testTransfer();
        testAlreadyRunning();
        testStartWebServerWithConnection();
        testAutoComplete();
    }

    private void testAlreadyRunning() throws Exception {
        Server server = Server.createWebServer("-webPort", "8182", "-properties", "null");
        server.start();
        assertTrue(server.getStatus().indexOf("server running") >= 0);
        Server server2 = Server.createWebServer("-webPort", "8182", "-properties", "null");
        assertEquals("Not started", server2.getStatus());
        try {
            server2.start();
            fail();
        } catch (Exception e) {
            assertTrue(e.toString().indexOf("port may be in use") >= 0);
            assertTrue(server2.getStatus().indexOf("could not be started") >= 0);
        }
        server.stop();
    }

    private void testTools() throws Exception {
        if (config.memory) {
            return;
        }
        deleteDb("web");
        Connection conn = getConnection("web");
        conn.createStatement().execute("create table test(id int) as select 1");
        conn.close();
        Server server = new Server();
        server.setOut(new PrintStream(new ByteArrayOutputStream()));
        server.runTool("-web", "-webPort", "8182", "-properties", "null", "-tcp", "-tcpPort", "9001");
        try {
            String url = "http://localhost:8182";
            WebClient client;
            String result;
            client = new WebClient();
            result = client.get(url);
            client.readSessionId(result);
            result = client.get(url, "tools.jsp");
            IOUtils.delete(getBaseDir() + "/backup.zip");
            result = client.get(url, "tools.do?tool=Backup&args=-dir," + getBaseDir() + ",-db,web,-file," + getBaseDir() + "/backup.zip");
            deleteDb("web");
            assertTrue(IOUtils.exists(getBaseDir() + "/backup.zip"));
            result = client.get(url, "tools.do?tool=DeleteDbFiles&args=-dir," + getBaseDir() + ",-db,web");
            assertFalse(IOUtils.exists(getBaseDir() + "/web.h2.db"));
            result = client.get(url, "tools.do?tool=Restore&args=-dir," + getBaseDir() + ",-db,web,-file," + getBaseDir() + "/backup.zip");
            assertTrue(IOUtils.exists(getBaseDir() + "/web.h2.db"));
            IOUtils.delete(getBaseDir() + "/web.h2.sql");
            IOUtils.delete(getBaseDir() + "/backup.zip");
            result = client.get(url, "tools.do?tool=Recover&args=-dir," + getBaseDir() + ",-db,web");
            assertTrue(IOUtils.exists(getBaseDir() + "/web.h2.sql"));
            IOUtils.delete(getBaseDir() + "/web.h2.sql");
            result = client.get(url, "tools.do?tool=RunScript&args=-script," + getBaseDir() + "/web.h2.sql,-url," + getURL("web", true) + ",-user," + getUser() + ",-password," + getPassword());
            IOUtils.delete(getBaseDir() + "/web.h2.sql");
            assertTrue(IOUtils.exists(getBaseDir() + "/web.h2.db"));
            deleteDb("web");
        } finally {
            server.shutdown();
        }
    }

    private void testTransfer() throws Exception {
        Server server = new Server();
        server.setOut(new PrintStream(new ByteArrayOutputStream()));
        server.runTool("-web", "-webPort", "8182", "-properties", "null");
        File transfer = new File("transfer");
        transfer.mkdirs();
        try {
            FileOutputStream f = new FileOutputStream("transfer/test.txt");
            f.write("Hello World".getBytes());
            f.close();
            WebClient client = new WebClient();
            String url = "http://localhost:8182";
            String result = client.get(url);
            client.readSessionId(result);
            String test = client.get(url, "transfer/test.txt");
            assertEquals("Hello World", test);
            new File("transfer/testUpload.txt").delete();
            client.upload(url + "/transfer/testUpload.txt", "testUpload.txt", new ByteArrayInputStream("Hallo Welt".getBytes()));
            byte[] d = IOUtils.readBytesAndClose(new FileInputStream("transfer/testUpload.txt"), -1);
            assertEquals("Hallo Welt", new String(d));
            new File("transfer/testUpload.txt").delete();
        } finally {
            server.shutdown();
            FileSystem.getInstance("transfer").deleteRecursive("transfer", true);
        }
    }

    private void testAutoComplete() throws Exception {
        Server server = new Server();
        server.setOut(new PrintStream(new ByteArrayOutputStream()));
        server.runTool("-web", "-webPort", "8182", "-properties", "null", "-tcp", "-tcpPort", "9001");
        String url = "http://localhost:8182";
        WebClient client;
        String result;
        client = new WebClient();
        client.setAcceptLanguage("de-de,de;q=0.5");
        result = client.get(url);
        client.readSessionId(result);
        result = client.get(url, "login.jsp");
        assertContains(result, "Einstellung");

        client = new WebClient();
        result = client.get(url);
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
        result = client.get(url, "query.do?sql=@list select * from test");
        assertContains(result, "Row #");
        result = client.get(url, "query.do?sql=@parameter_meta select * from test where id = ?");
        assertContains(result, "INTEGER");
        result = client.get(url, "query.do?sql=@edit select * from test");
        assertContains(result, "editResult.do");
        result = client.get(url, "query.do?sql=" + StringUtils.urlEncode("select space(100001) a, 1 b"));
        assertContains(result, "...");
        result = client.get(url, "query.do?sql=" + StringUtils.urlEncode("call '<&>'"));
        assertContains(result, "&lt;&amp;&gt;");
        result = client.get(url, "query.do?sql=@HISTORY");
        result = client.get(url, "getHistory.do?id=4");
        assertContains(result, "select * from test");
        result = client.get(url, "autoCompleteList.do?query=select 'abc");
        assertContains(StringUtils.urlDecode(result), "'");
        result = client.get(url, "autoCompleteList.do?query=select 'abc''");
        assertContains(StringUtils.urlDecode(result), "'");
        result = client.get(url, "autoCompleteList.do?query=select 'abc' ");
        assertContains(StringUtils.urlDecode(result), "||");
        result = client.get(url, "autoCompleteList.do?query=select 'abc' |");
        assertContains(StringUtils.urlDecode(result), "|");
        result = client.get(url, "autoCompleteList.do?query=select 'abc' || ");
        assertContains(StringUtils.urlDecode(result), "'");
        result = client.get(url, "autoCompleteList.do?query=call timestamp '2");
        assertContains(result, "20");
        result = client.get(url, "autoCompleteList.do?query=call time '1");
        assertContains(StringUtils.urlDecode(result), "12:00:00");
        result = client.get(url, "autoCompleteList.do?query=call timestamp '2001-01-01 12:00:00.");
        assertContains(result, "nanoseconds");
        result = client.get(url, "autoCompleteList.do?query=call timestamp '2001-01-01 12:00:00.00");
        assertContains(result, "nanoseconds");
        result = client.get(url, "autoCompleteList.do?query=call $$ hello world");
        assertContains(StringUtils.urlDecode(result), "$$");
        result = client.get(url, "autoCompleteList.do?query=alter index ");
        assertContains(StringUtils.urlDecode(result), "character");
        result = client.get(url, "autoCompleteList.do?query=alter index idx");
        assertContains(StringUtils.urlDecode(result), "character");
        result = client.get(url, "autoCompleteList.do?query=alter index \"IDX_");
        assertContains(StringUtils.urlDecode(result), "\"");
        result = client.get(url, "autoCompleteList.do?query=alter index \"IDX_\"\"");
        assertContains(StringUtils.urlDecode(result), "\"");
        result = client.get(url, "autoCompleteList.do?query=help ");
        assertContains(result, "anything");
        result = client.get(url, "autoCompleteList.do?query=help select");
        assertContains(result, "anything");
        result = client.get(url, "autoCompleteList.do?query=call ");
        assertContains(result, "0x");
        result = client.get(url, "autoCompleteList.do?query=call 0");
        assertContains(result, "0x");
        result = client.get(url, "autoCompleteList.do?query=call 0x");
        assertContains(result, "hex character");
        result = client.get(url, "autoCompleteList.do?query=call 0x123");
        assertContains(result, "hex character");
        result = client.get(url, "autoCompleteList.do?query=se");
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
        result = client.get(url, "autoCompleteList.do?query=select * from test where name = '");
        assertContains(StringUtils.urlDecode(result), "'");
        result = client.get(url, "autoCompleteList.do?query=select * from information_schema.columns where columns.");
        assertContains(result, "column_name");

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
        result = client.get(url, "login.do?driver=org.h2.Driver&url=jdbc:h2:mem:web&user=sa&password=sa&name=_test_");

        result = client.get(url, "logout.do");
        result = client.get(url, "settingRemove.do?name=_test_");

        client.get(url, "admin.do");
        try {
            client.get(url, "adminShutdown.do");
        } catch (IOException e) {
            // expected
            Thread.sleep(100);
        }
        server.shutdown();
        // it should be stopped now
        server = Server.createTcpServer("-tcpPort", "9001");
        server.start();
        server.stop();
    }

    private void testStartWebServerWithConnection() throws Exception {
        String old = System.getProperty(SysProperties.H2_BROWSER);
        try {
            System.setProperty(SysProperties.H2_BROWSER, "call:" + TestWeb.class.getName() + ".openBrowser");
            Server.openBrowser("testUrl");
            assertEquals("testUrl", lastUrl);
            String oldUrl = lastUrl;
            final Connection conn = getConnection("testWeb");
            Task t = new Task() {
                public void call() throws Exception {
                    Server.startWebServer(conn);
                }
            };
            t.execute();
            for (int i = 0; lastUrl == oldUrl; i++) {
                if (i > 100) {
                    throw new Exception("Browser not started");
                }
                Thread.sleep(100);
            }
            String url = lastUrl;
            WebClient client = new WebClient();
            client.readSessionId(url);
            url = client.getBaseUrl(url);
            try {
                client.get(url, "logout.do");
            } catch (Exception e) {
                // the server stopps on logout
            }
            t.get();
        } finally {
            if (old != null) {
                System.setProperty(SysProperties.H2_BROWSER, old);
            } else {
                System.clearProperty(SysProperties.H2_BROWSER);
            }
            deleteDb("testWeb");
        }
    }

    /**
     * This method is called via reflection.
     *
     * @param url the browser url
     */
    public static void openBrowser(String url) {
        lastUrl = url;
    }

}
