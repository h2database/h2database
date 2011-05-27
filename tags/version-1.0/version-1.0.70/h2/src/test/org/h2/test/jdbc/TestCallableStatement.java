/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (license2)
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.h2.test.TestBase;

/**
 * Tests for the CallableStatement class.
 */
public class TestCallableStatement extends TestBase {

    public void test() throws Exception {
        deleteDb("callableStatement");
        Connection conn = getConnection("preparedStatement");
        testPrepare(conn);
        conn.close();
    }

    private void testPrepare(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        CallableStatement call;
        ResultSet rs;
        stat.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR)");
        call = conn.prepareCall("INSERT INTO TEST VALUES(?, ?)");
        call.setInt(1, 1);
        call.setString(2, "Hello");
        call.execute();
        call = conn.prepareCall("SELECT * FROM TEST", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        rs = call.executeQuery();
        rs.next();
        check(1, rs.getInt(1));
        check("Hello", rs.getString(2));
        checkFalse(rs.next());
        call = conn.prepareCall("SELECT * FROM TEST", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        rs = call.executeQuery();
        rs.next();
        check(1, rs.getInt(1));
        check("Hello", rs.getString(2));
        checkFalse(rs.next());
    }

}
