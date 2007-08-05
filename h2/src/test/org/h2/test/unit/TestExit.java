/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.h2.api.DatabaseEventListener;
import org.h2.test.TestBase;

public class TestExit extends TestBase implements DatabaseEventListener {

    public void test() throws Exception {
        if(config.codeCoverage) {
            return;
        }
        String classPath = "bin"+File.pathSeparator+".";
        
        deleteDb("exit");
        String[] procDef;
        procDef = new String[]{
                "java", "-cp", classPath,
                getClass().getName(),
                "" + OPEN_WITH_CLOSE_ON_EXIT
        };
        Process proc = Runtime.getRuntime().exec(procDef);
        while (true) {
            int ch = proc.getErrorStream().read();
            if (ch < 0) {
                break;
            }
            System.out.print((char) ch);
        }
        while (true) {
            int ch = proc.getInputStream().read();
            if (ch < 0) {
                break;
            }
            System.out.print((char) ch);
        }
        proc.waitFor();
        if(!getClosedFile().exists()) {
            error("did not close database");
        }
        procDef = new String[]{
                "java", "-cp", classPath,
                getClass().getName(),
                "" + OPEN_WITHOUT_CLOSE_ON_EXIT
        };
        proc = Runtime.getRuntime().exec(procDef);
        proc.waitFor();
        if(getClosedFile().exists()) {
            error("closed database");
        }
    }
    
    static final int OPEN_WITH_CLOSE_ON_EXIT = 1, OPEN_WITHOUT_CLOSE_ON_EXIT = 2;
    
    public static Connection conn;
    
    public static void main(String[] args) throws Exception {
        if(args.length==0) {
            System.exit(1);
        }
        int action = Integer.parseInt(args[0]);
        TestExit app = new TestExit();
        app.execute(action);
    }
    
    void execute(int action) throws Exception {
        Class.forName("org.h2.Driver");
        String url = "";
        switch(action) {
        case OPEN_WITH_CLOSE_ON_EXIT:
            url = "jdbc:h2:"+BASE_DIR+"/exit;database_event_listener='" + getClass().getName() + "';db_close_on_exit=true";
            break;
        case OPEN_WITHOUT_CLOSE_ON_EXIT:
            url = "jdbc:h2:"+BASE_DIR+"/exit;database_event_listener='" + getClass().getName() + "';db_close_on_exit=false";
            break;
        }
        conn = open(url);
        Connection conn2 = open(url);
        conn2.close();
    }
    
    private static Connection open(String url) throws Exception {
        getClosedFile().delete();
        return DriverManager.getConnection(url, "sa", "");
    }

    public void diskSpaceIsLow(long stillAvailable) throws SQLException {
    }

    public void exceptionThrown(SQLException e, String sql) {
    }

    public void closingDatabase() {
        try {
            getClosedFile().createNewFile();
        } catch(IOException e) {
            TestBase.logError("error", e);
        }
    }
    
    private static File getClosedFile() {
        return new File(BASE_DIR + "/closed.txt");
    }

    public void setProgress(int state, String name, int x, int max) {       
    }

    public void init(String url) {
    }

}
