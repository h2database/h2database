/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.h2.store.FileLister;
import org.h2.test.TestBase;

public class TestReadOnly extends TestBase {

    public void test() throws Exception {
        if(config.memory) {
            return;
        }
        deleteDb("readonly");
        Connection conn = getConnection("readonly");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        stat.execute("INSERT INTO TEST VALUES(2, 'World')");
        conn.close();
        
        setReadOnly();
        conn = getConnection("readonly");
        stat = conn.createStatement();
        stat.execute("SELECT * FROM TEST");
        try {
            stat.execute("DELETE FROM TEST");
            error("read only delete");
        } catch(SQLException e) {
            checkNotGeneralException(e);
        }
        conn.close();
        
        conn = getConnection("readonly;DB_CLOSE_DELAY=1");
        stat = conn.createStatement();
        stat.execute("SELECT * FROM TEST");
        try {
            stat.execute("DELETE FROM TEST");
            error("read only delete");
        } catch(SQLException e) {
            checkNotGeneralException(e);
        }
        conn.close();
    }
    
    private void setReadOnly() throws SQLException {
        ArrayList list = FileLister.getDatabaseFiles(TestBase.BASE_DIR, "readonly", true);
        for(int i=0; i<list.size(); i++) {
            String fileName = (String) list.get(i);
            File file = new File(fileName);
            file.setReadOnly();
        }
    }

}
