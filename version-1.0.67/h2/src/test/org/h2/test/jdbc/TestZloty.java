/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.h2.test.TestBase;

/**
 * Tests a custom BigDecimal implementation, as well
 * as direct modification of a byte in a byte array.
 */
public class TestZloty extends TestBase {

    public void test() throws Exception {
        testZloty();
        testModifyBytes();
    }

    private static class ZlotyBigDecimal extends BigDecimal {

        public ZlotyBigDecimal(String s) {
            super(s);
        }

        private static final long serialVersionUID = -8004563653683501484L;

        public int compareTo(BigDecimal bd) {
            return -super.compareTo(bd);
        }

    }

    private void testModifyBytes() throws Exception {
        deleteDb("zloty");
        Connection conn = getConnection("zloty");
        conn.createStatement().execute("CREATE TABLE TEST(ID INT, DATA BINARY)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        byte[] shared = new byte[1];
        prep.setInt(1, 0);
        prep.setBytes(2, shared);
        prep.execute();
        shared[0] = 1;
        prep.setInt(1, 1);
        prep.setBytes(2, shared);
        prep.execute();
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        check(rs.getInt(1), 0);
        check(rs.getBytes(2)[0], 0);
        rs.next();
        check(rs.getInt(1), 1);
        check(rs.getBytes(2)[0], 1);
        rs.getBytes(2)[0] = 2;
        check(rs.getBytes(2)[0], 1);
        checkFalse(rs.next());
        conn.close();
    }

    /**
     * H2 destroyer application ;->
     *
     * @author Maciej Wegorkiewicz
     */
    private void testZloty() throws Exception {
        deleteDb("zloty");
        Connection conn = getConnection("zloty");
        conn.createStatement().execute("CREATE TABLE TEST(ID INT, AMOUNT DECIMAL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        prep.setInt(1, 1);
        prep.setBigDecimal(2, new BigDecimal("10.0"));
        prep.execute();
        prep.setInt(1, 2);
        try {
            prep.setBigDecimal(2, new ZlotyBigDecimal("11.0"));
            prep.execute();
            error();
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }

        prep.setInt(1, 3);
        try {
            BigDecimal value = new BigDecimal("12.100000") {
                private static final long serialVersionUID = -7909023971521750844L;

                public String toString() {
                    return "12,100000 EURO";
                }
            };
            prep.setBigDecimal(2, value);
            prep.execute();
            error();
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }

        conn.close();
    }

}
