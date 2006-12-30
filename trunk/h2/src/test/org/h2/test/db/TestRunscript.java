/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.h2.api.Trigger;
import org.h2.test.TestBase;

public class TestRunscript extends TestBase implements Trigger {

    public void test() throws Exception {
        test(false);
        test(true);
    }

    public static int test(int a) {
        return Math.abs(a);
    }

    private void test(boolean password) throws Exception {

        deleteDb("runscript");
        Connection conn1, conn2;
        Statement stat1, stat2;
        conn1 = getConnection("runscript");
        stat1 = conn1.createStatement();
        stat1.execute("create table test (id identity, name varchar(12))");
        stat1.execute("insert into test (name) values ('first'), ('second')");
        stat1.execute("create sequence testseq start with 100 increment by 10");
        stat1.execute("create alias mytest for \""+getClass().getName()+".test\"");
        stat1.execute("create trigger mytrigger before insert on test nowait call \""+getClass().getName()+"\"");
        stat1.execute("create view testview as select * from test where 1=0 union all select * from test where 0=1");
        stat1.execute("create user testadmin salt '00' hash '01' admin");
        stat1.execute("create schema testschema authorization testadmin");
        stat1.execute("create table testschema.parent(id int primary key, name varchar)");
        stat1.execute("create index idxname on testschema.parent(name)");
        stat1.execute("create table testschema.child(id int primary key, parentId int, name varchar, foreign key(parentId) references parent(id))");
        stat1.execute("create user testuser salt '02' hash '03'");
        stat1.execute("create role testrole");
        stat1.execute("grant all on testschema.child to testuser");
        stat1.execute("grant select, insert on testschema.parent to testrole");
        stat1.execute("grant testrole to testuser");

        String sql = "script to '"+BASE_DIR+"/backup.2.sql'";
        if(password) {
            sql += " CIPHER AES PASSWORD 't1e2s3t4'";
        }
        stat1.execute(sql);

        deleteDb("runscriptRestore");
        conn2 = getConnection("runscriptRestore");
        stat2 = conn2.createStatement();
        sql = "runscript from '"+BASE_DIR+"/backup.2.sql'";
        if(password) {
            sql += " CIPHER AES PASSWORD 'wrongpassword'";
        }
        if(password) {
            try {
                stat2.execute(sql);
                error("should fail");
            } catch(SQLException e) {
                checkNotGeneralException(e);
            }
        }
        sql = "runscript from '"+BASE_DIR+"/backup.2.sql'";
        if(password) {
            sql += " CIPHER AES PASSWORD 't1e2s3t4'";
        }
        stat2.execute(sql);
        stat2.execute("script to '"+BASE_DIR+"/backup.3.sql'");

        compareDatabases(stat1, stat2);

        conn1.close();
        conn2.close();
    }

    private void compareDatabases(Statement stat1, Statement stat2) throws Exception {
        ResultSet rs1 = stat1.executeQuery("SCRIPT NOPASSWORDS");
        ResultSet rs2 = stat2.executeQuery("SCRIPT NOPASSWORDS");
        ArrayList list1 = new ArrayList();
        ArrayList list2 = new ArrayList();
        while(rs1.next()) {
            check(rs2.next());
            list1.add(rs1.getString(1));
            list2.add(rs2.getString(1));
        }
        for(int i=0; i<list1.size(); i++) {
            String s = (String)list1.get(i);
            if(!list2.remove(s)) {
                error("not found: " + s);
            }
        }
        check(list2.size(), 0);
        checkFalse(rs2.next());
    }

    public void init(Connection conn, String schemaName, String triggerName, String tableName) {
    }

    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
    }

}
