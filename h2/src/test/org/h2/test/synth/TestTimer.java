/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.h2.test.TestBase;
import org.h2.tools.Backup;
import org.h2.tools.DeleteDbFiles;

public class TestTimer extends TestBase {

    public void test() throws Exception {
        validateOld();
        DeleteDbFiles.execute(BASE_DIR, "timer", true);
        loop();
    }

    private void loop() throws Exception {
        println("loop");
        Connection conn = getConnection("timer");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID IDENTITY, NAME VARCHAR)");
        Random random = new Random();
        int max = 0;
        int count = 0;
        long start = System.currentTimeMillis();
        while(true) {
            int action = random.nextInt(10);
            int x = max == 0 ? 0 : random.nextInt(max);
            switch(action) {
            case 0:
            case 1:
            case 2:
                stat.execute("INSERT INTO TEST VALUES(NULL, 'Hello')");
                ResultSet rs = stat.getGeneratedKeys();
                rs.next();
                int i = rs.getInt(1);
                max = i;
                count++;
                break;
            case 3:
            case 4:
                if(count == 0) {
                    break;
                }
                stat.execute("UPDATE TEST SET NAME=NAME||'+' WHERE ID=" + x);
                break;
            case 5:
            case 6:
                if(count == 0) {
                    break;
                }
                count -= stat.executeUpdate("DELETE FROM TEST WHERE ID=" + x);
                break;
            case 7:
                rs = stat.executeQuery("SELECT COUNT(*) FROM TEST");
                rs.next();
                int c = rs.getInt(1);
                check(c, count);
                long time = System.currentTimeMillis();
                if(time > start + 5000) {
                    println("rows: " + count);
                    start = time;
                }
                break;
            }
        }
    }

    private void validateOld() {
        println("validate");
        try {
            Connection conn = getConnection("timer");
            int todoValidateTransactions;
            Statement stat = conn.createStatement();
            ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM TEST");
            rs.next();
            int count = rs.getInt(1);
            conn.close();
            println("done, rows: " + count);
        } catch(Throwable e) {
            logError("validate", e);
            backup();
        }
    }

    private void backup() {
        println("backup");
        for(int i=0;; i++) {
            String s = "timer." + i + ".zip";
            File f = new File(s);
            if(f.exists()) {
                continue;
            }
            try {
                Backup.execute(s, BASE_DIR, "timer", true);
            } catch (SQLException e) {
                logError("backup", e);
            }
            break;
        }
    }

}
