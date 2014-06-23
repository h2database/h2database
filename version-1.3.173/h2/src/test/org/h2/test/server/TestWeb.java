/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
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
import java.sql.SQLException;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.test.utils.AssertThrows;
import org.h2.tools.Server;
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

    @Override
    public void test() throws Exception {
        testWrongParameters();
        testTools();
        testTransfer();
        testAlreadyRunning();
        testStartWebServerWithConnection();
        testServer();
        testWebApp();
    }

    private static void testWrongParameters() {
        new AssertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1) {
            @Override
            public void test() throws SQLException {
                Server.createPgServer("-pgPort 8182");
        }};
        new AssertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1) {
            @Override
            public void test() throws SQLException {
                Server.createTcpServer("-tcpPort 8182");
        }};
        new AssertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1) {
            @Override
            public void test() throws SQLException {
            Server.createWebServer("-webPort=8182");
        }};
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
        server.runTool("-web", "-webPort", "8182", "-properties", "null", "-tcp", "-tcpPort", "9101");
        try {
            String url = "http://localhost:8182";
            WebClient client;
            String result;
            client = new WebClient();
            result = client.get(url);
            client.readSessionId(result);
            result = client.get(url, "tools.jsp");
            FileUtils.delete(getBaseDir() + "/backup.zip");
            result = client.get(url, "tools.do?tool=Backup&args=-dir," +
                    getBaseDir() + ",-db,web,-file," + getBaseDir() + "/backup.zip");
            deleteDb("web");
            assertTrue(FileUtils.exists(getBaseDir() + "/backup.zip"));
            result = client.get(url, "tools.do?tool=DeleteDbFiles&args=-dir," +
                    getBaseDir() + ",-db,web");
            assertFalse(FileUtils.exists(getBaseDir() + "/web.h2.db"));
            result = client.get(url, "tools.do?tool=Restore&args=-dir," +
                    getBaseDir() + ",-db,web,-file," + getBaseDir() + "/backup.zip");
            assertTrue(FileUtils.exists(getBaseDir() + "/web.h2.db"));
            FileUtils.delete(getBaseDir() + "/web.h2.sql");
            FileUtils.delete(getBaseDir() + "/backup.zip");
            result = client.get(url, "tools.do?tool=Recover&args=-dir," +
                    getBaseDir() + ",-db,web");
            assertTrue(FileUtils.exists(getBaseDir() + "/web.h2.sql"));
            FileUtils.delete(getBaseDir() + "/web.h2.sql");
            result = client.get(url, "tools.do?tool=RunScript&args=-script," +
                    getBaseDir() + "/web.h2.sql,-url," + getURL("web", true) +
                    ",-user," + getUser() + ",-password," + getPassword());
            FileUtils.delete(getBaseDir() + "/web.h2.sql");
            assertTrue(FileUtils.exists(getBaseDir() + "/web.h2.db"));
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
            FileUtils.deleteRecursive("transfer", true);
        }
    }

    private void testServer() throws Exception {
        Server server = new Server();
        server.setOut(new PrintStream(new ByteArrayOutputStream()));
        server.runTool("-web", "-webPort", "8182", "-properties", "null", "-tcp", "-tcpPort", "9101");
        try {
            String url = "http://localhost:8182";
            WebClient client;
            String result;
            client = new WebClient();
            client.setAcceptLanguage("de-de,de;q=0.5");
            result = client.get(url);
            client.readSessionId(result);
            result = client.get(url, "login.jsp");
            assertEquals("text/html", client.getContentType());
            assertContains(result, "Einstellung");
            client.get(url, "favicon.ico");
            assertEquals("image/x-icon", client.getContentType());
            client.get(url, "ico_ok.gif");
            assertEquals("image/gif", client.getContentType());
            client.get(url, "tree.js");
            assertEquals("text/javascript", client.getContentType());
            client.get(url, "stylesheet.css");
            assertEquals("text/css", client.getContentType());
            client.get(url, "admin.do");
            try {
                client.get(url, "adminShutdown.do");
            } catch (IOException e) {
                // expected
                Thread.sleep(1000);
            }
        } finally {
            server.shutdown();
        }
        // it should be stopped now
        server = Server.createTcpServer("-tcpPort", "9101");
        server.start();
        server.stop();
    }

    private void testWebApp() throws Exception {
        Server server = new Server();
        server.setOut(new PrintStream(new ByteArrayOutputStream()));
        server.runTool("-web", "-webPort", "8182", "-properties", "null", "-tcp", "-tcpPort", "9101");
        try {
            String url = "http://localhost:8182";
            WebClient client;
            String result;
            client = new WebClient();
            result = client.get(url);
            client.readSessionId(result);
            client.get(url, "login.jsp");
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
            result = client.get(url, "query.do?sql=create table test(id int primary key, name varchar);" +
                    "insert into test values(1, 'Hello')");
            result = client.get(url, "query.do?sql=create sequence test_sequence");
            result = client.get(url, "query.do?sql=create schema test_schema");
            result = client.get(url, "query.do?sql=create view test_view as select * from test");
            result = client.get(url, "tables.do");
            result = client.get(url, "query.jsp");
            result = client.get(url, "query.do?sql=select * from test");
            assertContains(result, "Hello");
            result = client.get(url, "query.do?sql=select * from test");
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
            result = client.get(url, "query.do?sql=delete from test");
            // op 1 (row -1: insert, otherwise update): ok, 2: delete  3: cancel,
            result = client.get(url, "editResult.do?sql=@edit select * from test&op=1&row=-1&r-1c1=1&r-1c2=Hello");
            assertContains(result, "1");
            assertContains(result, "Hello");
            result = client.get(url, "editResult.do?sql=@edit select * from test&op=1&row=1&r1c1=1&r1c2=Hallo");
            assertContains(result, "1");
            assertContains(result, "Hallo");
            result = client.get(url, "query.do?sql=select * from test");
            assertContains(result, "1");
            assertContains(result, "Hallo");
            result = client.get(url, "editResult.do?sql=@edit select * from test&op=2&row=1");
            result = client.get(url, "query.do?sql=select * from test");
            assertContains(result, "no rows");

            // autoComplete
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
            assertContains(result, ".");
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

            // special commands
            result = client.get(url, "query.do?sql=@autocommit_true");
            assertContains(result, "Auto commit is now ON");
            result = client.get(url, "query.do?sql=@autocommit_false");
            assertContains(result, "Auto commit is now OFF");
            result = client.get(url, "query.do?sql=@cancel");
            assertContains(result, "There is currently no running statement");
            result = client.get(url, "query.do?sql=@generated insert into test(id) values(test_sequence.nextval)");
            assertContains(result, "SCOPE_IDENTITY()");
            result = client.get(url, "query.do?sql=@maxrows 2000");
            assertContains(result, "Max rowcount is set");
            result = client.get(url, "query.do?sql=@password_hash user password");
            assertContains(result, "501cf5c163c184c26e62e76d25d441979f8f25dfd7a683484995b4a43a112fdf");
            result = client.get(url, "query.do?sql=@sleep 1");
            assertContains(result, "Ok");
            result = client.get(url, "query.do?sql=@catalogs");
            assertContains(result, "PUBLIC");
            result = client.get(url, "query.do?sql=@column_privileges null null null TEST null");
            assertContains(result, "PRIVILEGE");
            result = client.get(url, "query.do?sql=@cross_references null null null TEST");
            assertContains(result, "PKTABLE_NAME");
            result = client.get(url, "query.do?sql=@exported_keys null null null TEST");
            assertContains(result, "PKTABLE_NAME");
            result = client.get(url, "query.do?sql=@imported_keys null null null TEST");
            assertContains(result, "PKTABLE_NAME");
            result = client.get(url, "query.do?sql=@primary_keys null null null TEST");
            assertContains(result, "PK_NAME");
            result = client.get(url, "query.do?sql=@procedures null null null");
            assertContains(result, "PROCEDURE_NAME");
            result = client.get(url, "query.do?sql=@procedure_columns");
            assertContains(result, "PROCEDURE_NAME");
            result = client.get(url, "query.do?sql=@schemas");
            assertContains(result, "PUBLIC");
            result = client.get(url, "query.do?sql=@table_privileges");
            assertContains(result, "PRIVILEGE");
            result = client.get(url, "query.do?sql=@table_types");
            assertContains(result, "SYSTEM TABLE");
            result = client.get(url, "query.do?sql=@type_info");
            assertContains(result, "CLOB");
            result = client.get(url, "query.do?sql=@version_columns");
            assertContains(result, "PSEUDO_COLUMN");
            result = client.get(url, "query.do?sql=@attributes");
            assertContains(result, "Feature not supported: &quot;attributes&quot;");
            result = client.get(url, "query.do?sql=@super_tables");
            assertContains(result, "SUPERTABLE_NAME");
            result = client.get(url, "query.do?sql=@super_types");
            assertContains(result, "Feature not supported: &quot;superTypes&quot;");
            result = client.get(url, "query.do?sql=@prof_start");
            assertContains(result, "Ok");
            result = client.get(url, "query.do?sql=@prof_stop");
            assertContains(result, "Top Stack Trace(s)");
            result = client.get(url, "query.do?sql=@best_row_identifier null null TEST");
            assertContains(result, "SCOPE");
            assertContains(result, "COLUMN_NAME");
            assertContains(result, "ID");
            result = client.get(url, "query.do?sql=@udts");
            assertContains(result, "CLASS_NAME");
            result = client.get(url, "query.do?sql=@udts null null null 1,2,3");
            assertContains(result, "CLASS_NAME");
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

            result = client.get(url, "query.do?sql=@INFO");
            assertContains(result, "getCatalog");

            result = client.get(url, "logout.do");
            result = client.get(url, "login.do?driver=org.h2.Driver&url=jdbc:h2:mem:web&user=sa&password=sa&name=_test_");

            result = client.get(url, "logout.do");
            result = client.get(url, "settingRemove.do?name=_test_");

            client.get(url, "admin.do");
        } finally {
            server.shutdown();
        }
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
                @Override
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
                // the server stops on logout
            }
            t.get();
            conn.close();
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
