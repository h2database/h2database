/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.File;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.h2.engine.Constants;
import org.h2.store.FileLister;
import org.h2.test.TestBase;

/**
 * Test for the read-only database feature.
 */
public class TestReadOnly extends TestBase {

    public void test() throws Exception {
        if (config.memory) {
            return;
        }
        testReadOnlyFiles(true);
        if (!config.deleteIndex) {
            testReadOnlyFiles(false);
        }
    }

    private void testReadOnlyFiles(boolean setReadOnly) throws Exception {

        File f = File.createTempFile("test", "temp");
        assertTrue(f.canWrite());
        f.setReadOnly();
        assertTrue(!f.canWrite());
        f.delete();

        f = File.createTempFile("test", "temp");
        RandomAccessFile r = new RandomAccessFile(f, "rw");
        r.write(1);
        f.setReadOnly();
        r.close();
        assertTrue(!f.canWrite());
        f.delete();

        deleteDb("readonly");
        Connection conn = getConnection("readonly");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        stat.execute("INSERT INTO TEST VALUES(2, 'World')");
        assertTrue(!conn.isReadOnly());
        conn.close();

        if (setReadOnly) {
            setReadOnly();
            conn = getConnection("readonly");
        } else {
            conn = getConnection("readonly;ACCESS_MODE_DATA=r");
        }
        assertTrue(conn.isReadOnly());
        stat = conn.createStatement();
        stat.execute("SELECT * FROM TEST");
        try {
            stat.execute("DELETE FROM TEST");
            fail("read only delete");
        } catch (SQLException e) {
            assertKnownException(e);
        }
        conn.close();

        if (setReadOnly) {
            conn = getConnection("readonly;DB_CLOSE_DELAY=1");
        } else {
            conn = getConnection("readonly;DB_CLOSE_DELAY=1;ACCESS_MODE_DATA=r");
        }
        stat = conn.createStatement();
        stat.execute("SELECT * FROM TEST");
        try {
            stat.execute("DELETE FROM TEST");
            fail("read only delete");
        } catch (SQLException e) {
            assertKnownException(e);
        }
        stat.execute("SET DB_CLOSE_DELAY=0");
        conn.close();
    }

    private void setReadOnly() throws SQLException {
        String lastLogFile = null;
        ArrayList list = FileLister.getDatabaseFiles(TestBase.baseDir, "readonly", true);
        for (int i = 0; i < list.size(); i++) {
            String fileName = (String) list.get(i);
            File file = new File(fileName);
            file.setReadOnly();
            if (fileName.endsWith(Constants.SUFFIX_LOG_FILE)) {
                if (lastLogFile == null || lastLogFile.compareTo(fileName) < 0) {
                    lastLogFile = fileName;
                }
            }
        }
        // delete all log files except the last one
        for (int i = 0; i < list.size(); i++) {
            String fileName = (String) list.get(i);
            if (fileName.endsWith(Constants.SUFFIX_LOG_FILE)) {
                if (!lastLogFile.equals(fileName)) {
                    File file = new File(fileName);
                    file.delete();
                }
            }
        }
    }

}
