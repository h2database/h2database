/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;

import org.h2.constant.SysProperties;
import org.h2.store.FileLister;
import org.h2.test.TestBase;

/**
 * Tests the database transaction log file.
 */
public class TestLogFile extends TestBase {

    private Connection conn;
    private static final int MAX_LOG_SIZE = 1;

    private long reconnect(int maxFiles) throws Exception {
        if (conn != null) {
            conn.close();
        }
        long length = 0;
        ArrayList files = FileLister.getDatabaseFiles(baseDir, "logfile", false);
        checkSmaller(files.size(), maxFiles + 2);
        for (int i = 0; i < files.size(); i++) {
            String fileName = (String) files.get(i);
            long len = new File(fileName).length();
            length += len;
        }
        conn = getConnection("logfile");
        return length;
    }

    public void test() throws Exception {
        if (config.memory) {
            return;
        }
        deleteDb("logfile");
        int old = SysProperties.getLogFileDeleteDelay();
        System.setProperty(SysProperties.H2_LOG_DELETE_DELAY, "0");
        try {
            reconnect(0);
            insert();
            int maxFiles = 3; // data, index, log
            for (int i = 0; i < 3; i++) {
                long length = reconnect(maxFiles);
                insert();
                long l2 = reconnect(maxFiles);
                trace("l2=" + l2);
                check(l2 <= length * 2);
            }
            conn.close();
        } finally {
            System.setProperty(SysProperties.H2_LOG_DELETE_DELAY, "" + old);
        }
    }

    private void checkLogSize() throws Exception {
        String[] files = new File(".").list();
        for (int j = 0; j < files.length; j++) {
            String name = files[j];
            if (name.startsWith("logfile") && name.endsWith(".log.db")) {
                long length = new File(name).length();
                checkSmaller(length, MAX_LOG_SIZE * 1024 * 1024 * 2);
            }
        }
    }

    void insert() throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("SET LOGSIZE 200");
        stat.execute("SET MAX_LOG_SIZE " + MAX_LOG_SIZE);
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, 'Hello' || ?)");
        int len = getSize(1, 10000);
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            prep.setInt(2, i);
            prep.execute();
            if (i > 0 && (i % 2000) == 0) {
                checkLogSize();
            }
        }
        checkLogSize();
    }

}
