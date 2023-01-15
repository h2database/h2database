/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Test using an encrypted database.
 */
public class TestEncryptedDb extends TestDb {

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
        if (config.memory || config.cipher != null) {
            return false;
        }
        return true;
    }

    @Override
    public void test() throws SQLException {
        deleteDb("encrypted");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1,
                () -> getConnection("encrypted;CIPHER=AES;PAGE_SIZE=2048", "sa", "1234 1234"));
        try (Connection conn = getConnection("encrypted;CIPHER=AES", "sa", "123 123")) {
            Statement stat = conn.createStatement();
            stat.execute("CREATE TABLE TEST(ID INT)");
            stat.execute("CHECKPOINT");
            stat.execute("SET WRITE_DELAY 0");
            stat.execute("INSERT INTO TEST VALUES(1)");
            stat.execute("SHUTDOWN IMMEDIATELY");
        }

        assertThrows(ErrorCode.FILE_ENCRYPTION_ERROR_1, //
                () -> getConnection("encrypted;CIPHER=AES", "sa", "1234 1234"));

        try (Connection conn = getConnection("encrypted;CIPHER=AES", "sa", "123 123")) {
            Statement stat = conn.createStatement();
            ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
        }
//        conn.close();
        deleteDb("encrypted");
    }

}
