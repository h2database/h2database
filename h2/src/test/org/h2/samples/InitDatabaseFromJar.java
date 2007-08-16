package org.h2.samples;

import java.io.*;
import java.sql.*;
import org.h2.tools.RunScript;

public class InitDatabaseFromJar {
    
    public static void main(String[] args) throws Exception {
        new InitDatabaseFromJar().createScript();        
        new InitDatabaseFromJar().initDb();
    }
    
    private void createScript() throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:test");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES('Hello World')");
        stat.execute("SCRIPT TO 'script.sql'");
        conn.close();
    }

    void initDb() throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:test");
        InputStream in = getClass().getResourceAsStream("script.sql");
        if(in == null) {
            System.out.println("Please add the file script.sql to the classpath, package " + getClass().getPackage().getName());
        } else {
            RunScript.execute(conn, new InputStreamReader(in));
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
            while(rs.next()) {
                System.out.println(rs.getString(1));
            }
            conn.close();
        }
    }
}
