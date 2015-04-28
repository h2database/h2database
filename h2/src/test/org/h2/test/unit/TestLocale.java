/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import org.h2.test.TestBase;

/**
 * Tests that change the default locale.
 */
public class TestLocale extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        // System.setProperty("h2.storeLocalTime", "true");
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException {
        testSpecialLocale();
    }

    private void testSpecialLocale() throws SQLException {
        deleteDb(getTestName());
        Connection conn = getConnection(getTestName());
        Statement stat = conn.createStatement();
        Locale old = Locale.getDefault();
        try {
            // when using Turkish as the default locale, "i".toUpperCase() is
            // not "I"
            Locale.setDefault(new Locale("tr"));
            stat.execute("create table test(I1 int, i2 int, b int, c int, d int) " +
                    "as select 1, 1, 1, 1, 1");
            ResultSet rs = stat.executeQuery("select * from test");
            rs.next();
            rs.getString("I1");
            rs.getString("i1");
            rs.getString("I2");
            rs.getString("i2");
            stat.execute("drop table test");
        } finally {
            Locale.setDefault(old);
        }
        conn.close();
    }

}
