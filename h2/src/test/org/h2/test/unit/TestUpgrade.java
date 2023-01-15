/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;

import org.h2.engine.Constants;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.tools.Upgrade;

/**
 * Tests upgrade utility.
 */
public class TestUpgrade extends TestBase {

    /**
     * Run just this test.
     *
     * @param a
     *            ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        deleteDb();
        testUpgrade(1, 2, 120);
        testUpgrade(1, 4, 200);
    }

    private void testUpgrade(int major, int minor, int build) throws Exception {
        String baseDir = getBaseDir();
        String url = "jdbc:h2:" + baseDir + "/testUpgrade";
        Properties p = new Properties();
        p.put("user", "sa");
        p.put("password", "password");
        Random r = new Random();
        byte[] bytes = new byte[10_000];
        r.nextBytes(bytes);
        String s = new String(bytes, StandardCharsets.ISO_8859_1);
        java.sql.Driver driver = Upgrade.loadH2(build);
        try {
            assertEquals(major, driver.getMajorVersion());
            assertEquals(minor, driver.getMinorVersion());
            try (Connection conn = driver.connect(url, p)) {
                Statement stat = conn.createStatement();
                stat.execute("CREATE TABLE TEST(ID BIGINT AUTO_INCREMENT PRIMARY KEY, B BINARY, L BLOB, C CLOB)");
                PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(B, L, C) VALUES (?, ?, ?)");
                prep.setBytes(1, bytes);
                prep.setBytes(2, bytes);
                prep.setString(3, s);
                prep.execute();
            }
        } finally {
            Upgrade.unloadH2(driver);
        }
        assertTrue(Upgrade.upgrade(url, p, build));
        try (Connection conn = DriverManager.getConnection(url, p)) {
            Statement stat = conn.createStatement();
            try (ResultSet rs = stat.executeQuery("TABLE TEST")) {
                assertTrue(rs.next());
                assertEquals(bytes, rs.getBytes(2));
                assertEquals(bytes, rs.getBytes(3));
                assertEquals(s, rs.getString(4));
                assertFalse(rs.next());
            }
            try (ResultSet rs = stat.executeQuery("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_OCTET_LENGTH"
                    + " FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION")) {
                assertTrue(rs.next());
                assertEquals("ID", rs.getString(1));
                assertEquals("BIGINT", rs.getString(2));
                assertTrue(rs.next());
                assertEquals("B", rs.getString(1));
                assertEquals("BINARY VARYING", rs.getString(2));
                assertEquals(Constants.MAX_STRING_LENGTH, rs.getLong(3));
                assertTrue(rs.next());
                assertEquals("L", rs.getString(1));
                assertEquals("BINARY LARGE OBJECT", rs.getString(2));
                assertEquals(Long.MAX_VALUE, rs.getLong(3));
                assertTrue(rs.next());
                assertEquals("C", rs.getString(1));
                assertEquals("CHARACTER LARGE OBJECT", rs.getString(2));
                assertEquals(Long.MAX_VALUE, rs.getLong(3));
                assertFalse(rs.next());
            }
        }
        deleteDb();
    }

    private void deleteDb() {
        for (FilePath p : FilePath.get(getBaseDir()).newDirectoryStream()) {
            if (p.getName().startsWith("testUpgrade")) {
                FileUtils.deleteRecursive(p.toString(), false);
            }
        }
    }

}
