/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;

import org.h2.api.Trigger;
import org.h2.test.TestBase;

/**
 * Tests for trigger and constraints.
 */
public class TestTriggersConstraints extends TestBase implements Trigger {

    private static boolean mustNotCallTrigger;

    public void test() throws Exception {
        deleteDb("trigger");
        testTriggers();
        testConstraints();
    }

    private void testConstraints() throws Exception {
        Connection conn = getConnection("trigger");
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("create table test(id int primary key, parent int)");
        stat.execute("alter table test add constraint test_parent_id foreign key(parent) references test (id) on delete cascade");
        stat.execute("insert into test select x, x/2 from system_range(0, 100)");
        stat.execute("delete from test");
        checkSingleValue(stat, "select count(*) from test", 0);
        stat.execute("drop table test");
        conn.close();
    }

    private void testTriggers() throws Exception {
        mustNotCallTrigger = false;
        Connection conn = getConnection("trigger");
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        // CREATE TRIGGER trigger {BEFORE|AFTER} {INSERT|UPDATE|DELETE} ON table
        // [FOR EACH ROW] [QUEUE n] [NOWAIT] CALL triggeredClass
        stat.execute("CREATE TRIGGER IF NOT EXISTS INS_BEFORE BEFORE INSERT ON TEST FOR EACH ROW NOWAIT CALL \""
                + getClass().getName() + "\"");
        stat.execute("CREATE TRIGGER IF NOT EXISTS INS_BEFORE BEFORE INSERT ON TEST FOR EACH ROW NOWAIT CALL \""
                + getClass().getName() + "\"");
        stat.execute("CREATE TRIGGER INS_AFTER AFTER INSERT ON TEST FOR EACH ROW NOWAIT CALL \"" + getClass().getName()
                + "\"");
        stat.execute("CREATE TRIGGER UPD_BEFORE BEFORE UPDATE ON TEST FOR EACH ROW NOWAIT CALL \""
                + getClass().getName() + "\"");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        ResultSet rs;
        rs = stat.executeQuery("SCRIPT");
        checkRows(rs, new String[] {
                "CREATE FORCE TRIGGER PUBLIC.INS_BEFORE BEFORE INSERT ON PUBLIC.TEST FOR EACH ROW NOWAIT CALL \""
                        + getClass().getName() + "\";",
                "CREATE FORCE TRIGGER PUBLIC.INS_AFTER AFTER INSERT ON PUBLIC.TEST FOR EACH ROW NOWAIT CALL \""
                        + getClass().getName() + "\";",
                "CREATE FORCE TRIGGER PUBLIC.UPD_BEFORE BEFORE UPDATE ON PUBLIC.TEST FOR EACH ROW NOWAIT CALL \""
                        + getClass().getName() + "\";" });
        while (rs.next()) {
            String sql = rs.getString(1);
            if (sql.startsWith("CREATE TRIGGER")) {
                System.out.println(sql);
            }
        }

        rs = stat.executeQuery("SELECT * FROM TEST");
        rs.next();
        check(rs.getString(2), "Hello-updated");
        checkFalse(rs.next());
        stat.execute("UPDATE TEST SET NAME=NAME||'-upd'");
        rs = stat.executeQuery("SELECT * FROM TEST");
        rs.next();
        check(rs.getString(2), "Hello-updated-upd-updated2");
        checkFalse(rs.next());

        mustNotCallTrigger = true;
        stat.execute("DROP TRIGGER IF EXISTS INS_BEFORE");
        stat.execute("DROP TRIGGER IF EXISTS INS_BEFORE");
        try {
            stat.execute("DROP TRIGGER INS_BEFORE");
            error();
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        stat.execute("DROP TRIGGER  INS_AFTER");
        stat.execute("DROP TRIGGER  UPD_BEFORE");
        stat.execute("UPDATE TEST SET NAME=NAME||'-upd-no_trigger'");
        stat.execute("INSERT INTO TEST VALUES(100, 'Insert-no_trigger')");
        conn.close();

        conn = getConnection("trigger");

        mustNotCallTrigger = false;
        conn.close();
    }

    private void checkRows(ResultSet rs, String[] expected) throws Exception {
        HashSet set = new HashSet(Arrays.asList(expected));
        while (rs.next()) {
            set.remove(rs.getString(1));
        }
        if (set.size() > 0) {
            error("set should be empty: " + set);
        }
    }

    private String triggerName;

    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
        if (mustNotCallTrigger) {
            throw new Error("must not be called now");
        }
        if (conn == null) {
            throw new Error("connection is null");
        }
        if (triggerName.startsWith("INS_BEFORE")) {
            newRow[1] = newRow[1] + "-updated";
        } else if (triggerName.startsWith("INS_AFTER")) {
            if (!newRow[1].toString().endsWith("-updated")) {
                throw new Error("supposed to be updated");
            }
            checkCommit(conn);
        } else if (triggerName.startsWith("UPD_BEFORE")) {
            newRow[1] = newRow[1] + "-updated2";
        } else if (triggerName.startsWith("UPD_AFTER")) {
            if (!newRow[1].toString().endsWith("-updated2")) {
                throw new Error("supposed to be updated2");
            }
            checkCommit(conn);
        }
    }
    
    private void checkCommit(Connection conn) {
        try {
            conn.commit();
            throw new Error("Commit must not work here");
        } catch (SQLException e) {
            try {
                checkNotGeneralException(e);
            } catch (Exception e2) {
                throw new Error("Unexpected: " + e.toString());
            }
        }
        try {
            conn.createStatement().execute("CREATE TABLE X(ID INT)");
            throw new Error("CREATE TABLE WORKED, but implicitly commits");
        } catch (SQLException e) {
            try {
                checkNotGeneralException(e);
            } catch (Exception e2) {
                throw new Error("Unexpected: " + e.toString());
            }
        }        
    }

    public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before, int type) throws SQLException {
        this.triggerName = triggerName;
        if (!"TEST".equals(tableName)) {
            throw new Error("supposed to be TEST");
        }
        if ((triggerName.endsWith("AFTER") && before) || (triggerName.endsWith("BEFORE") && !before)) {
            throw new Error("triggerName: " + triggerName + " before:" + before);
        }
        if ((triggerName.startsWith("UPD") && type != UPDATE) || (triggerName.startsWith("INS") && type != INSERT) || (triggerName.startsWith("DEL") && type != DELETE)) {
            throw new Error("triggerName: " + triggerName + " type:" + type);
        }
    }

}
