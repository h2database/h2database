/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.File;

import org.h2.api.ErrorCode;
import org.h2.engine.ConnectionInfo;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.tools.DeleteDbFiles;

/**
 * Test the ConnectionInfo class.
 *
 * @author Kerry Sainsbury
 * @author Thomas Mueller Graf
 */
public class TestConnectionInfo extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        testImplicitRelativePath();
        testConnectInitError();
        testConnectionInfo();
        testName();
    }

    private void testImplicitRelativePath() throws Exception {
        assertThrows(ErrorCode.URL_RELATIVE_TO_CWD, () -> getConnection("jdbc:h2:" + getTestName()));
        assertThrows(ErrorCode.URL_RELATIVE_TO_CWD, () -> getConnection("jdbc:h2:data/" + getTestName()));

        getConnection("jdbc:h2:./data/" + getTestName()).close();
        DeleteDbFiles.execute("data", getTestName(), true);
    }

    private void testConnectInitError() throws Exception {
        assertThrows(ErrorCode.SYNTAX_ERROR_2, () -> getConnection("jdbc:h2:mem:;init=error"));
        assertThrows(ErrorCode.IO_EXCEPTION_2, () -> getConnection("jdbc:h2:mem:;init=runscript from 'wrong.file'"));
    }

    private void testConnectionInfo() {
        ConnectionInfo connectionInfo = new ConnectionInfo(
                "jdbc:h2:mem:" + getTestName() +
                        ";ACCESS_MODE_DATA=rws" +
                        ";INIT=CREATE this...\\;INSERT that..." +
                        ";IFEXISTS=TRUE",
                null, null, null);

        assertEquals("jdbc:h2:mem:" + getTestName(),
                connectionInfo.getURL());

        assertEquals("rws",
                connectionInfo.getProperty("ACCESS_MODE_DATA", ""));
        assertEquals("CREATE this...;INSERT that...",
                connectionInfo.getProperty("INIT", ""));
        assertEquals("TRUE",
                connectionInfo.getProperty("IFEXISTS", ""));
        assertEquals("undefined",
                connectionInfo.getProperty("CACHE_TYPE", "undefined"));
    }

    private void testName() throws Exception {
        char differentFileSeparator = File.separatorChar == '/' ? '\\' : '/';
        ConnectionInfo connectionInfo = new ConnectionInfo("./test" +
                differentFileSeparator + "subDir");
        File file = new File("test" + File.separatorChar + "subDir");
        assertEquals(file.getCanonicalPath().replace('\\', '/'),
                connectionInfo.getName());
    }

}
