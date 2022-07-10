/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import static org.h2.engine.Constants.SUFFIX_MV_FILE;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.NumberFormat;

import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Test off-line compaction procedure used by SHUTDOWN DEFRAG command
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public class TestDefrag  extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public boolean isEnabled() {
        return !config.memory && config.big && !config.ci;
    }

    @Override
    public void test() throws Exception {
        String cipher = config.cipher;
        config.traceTest = true;
        try {
            config.cipher = null;
            testIt();
            config.cipher = "AES";
            testIt();
        } finally {
            config.cipher = cipher;
        }
    }

    public void testIt() throws Exception {
        String dbName = getTestName();
        deleteDb(dbName);
        File dbFile = new File(getBaseDir(), dbName + SUFFIX_MV_FILE);
        NumberFormat nf = NumberFormat.getInstance();
        try (Connection c = getConnection(dbName)) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE IF NOT EXISTS test (id INT PRIMARY KEY, txt varchar)" +
                            " AS SELECT x, x || SPACE(200) FROM SYSTEM_RANGE(1,10000000)");
                st.execute("checkpoint");
            }
            long origSize = dbFile.length();
            String message = "before defrag: " + nf.format(origSize);
            trace(message);
            assertTrue(message, origSize > 4_000_000_000L);
            try (Statement st = c.createStatement()) {
                st.execute("shutdown defrag");
            }
        }
        long compactedSize = dbFile.length();
        String message = "after defrag: " + nf.format(compactedSize);
        trace(message);
        assertTrue(message, compactedSize < 400_000_000L);

        try (Connection c = getConnection(dbName + ";LAZY_QUERY_EXECUTION=1")) {
            try (Statement st = c.createStatement()) {
                ResultSet rs = st.executeQuery("SELECT * FROM  test");
                int count = 0;
                while (rs.next()) {
                    ++count;
                    assertEquals(count, rs.getInt(1));
                    assertTrue(rs.getString(2).startsWith(count + "   "));
                }
                assertEquals(10_000_000, count);
            }
        }
        deleteDb(dbName);
    }
}
