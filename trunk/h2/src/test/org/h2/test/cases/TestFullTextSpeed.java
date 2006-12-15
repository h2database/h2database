/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.cases;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

import org.h2.tools.DeleteDbFiles;

public class TestFullTextSpeed {
    
    private static final int RECORD_COUNT = 100;
    private Connection conn;
    
    // java -Xrunhprof:cpu=samples,depth=6 org.h2.test.cases.TestFullTextSpeed
    
    public static void main(String[] args) throws Exception {
//        new TestFullTextSpeed().test(true);
        new TestFullTextSpeed().test(false);
//        new TestFullTextSpeed().test(true);
        new TestFullTextSpeed().test(false);
    }

    public void test(boolean lucene) throws Exception {
        String type = lucene ? "FTL_" : "FT_";
        String init = lucene ? "org.h2.fulltext.FullTextLucene.init" : "org.h2.fulltext.FullText.init";
        System.out.println((lucene ? "Lucene" : "Native") + " full text search");
        DeleteDbFiles.execute(null, "test", true);
        Class.forName("org.h2.Driver");
        conn = DriverManager.getConnection("jdbc:h2:test;ASSERT=FALSE;LOG=0", "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID IDENTITY, NAME VARCHAR)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(NAME) VALUES(?)");
        Random random = new Random(0);
        for(int i=0; i<RECORD_COUNT; i++) {
            prep.setString(1, getRandomText(random));
            prep.execute();
        }
        long time;
        time = System.currentTimeMillis();
        stat.execute("CREATE ALIAS IF NOT EXISTS "+type+"INIT FOR \""+init+"\"");
        stat.execute("CALL "+type+"INIT()");
        stat.execute("CALL "+type+"DROP_ALL()");
        stat.execute("CALL "+type+"CREATE_INDEX('PUBLIC', 'TEST', NULL)");
        System.out.println("index: " + (System.currentTimeMillis() - time));
        prep = conn.prepareStatement("SELECT * FROM "+type+"SEARCH(?, 0, 0)");
        time = System.currentTimeMillis();
        int totalResults = 0;
        for(int i=0; i<1000 * RECORD_COUNT; i++) {
            prep.setString(1, getRandomWord(random));
            ResultSet rs = prep.executeQuery();
            while(rs.next()) {
                rs.getString(1);
                totalResults++;
            }
        }
        System.out.println("query: " + (System.currentTimeMillis() - time) + " results: " +  totalResults);
        conn.close();
    }
    
    private String getRandomWord(Random random) {
        return "" + (char)(random.nextInt('z'-'a') + 'a') + random.nextInt(RECORD_COUNT);
    }

    private String getRandomText(Random random) {
        int words = random.nextInt(1000);
        StringBuffer buff = new StringBuffer(words * 6);
        for(int i=0; i<words; i++) {
            buff.append(getRandomWord(random));
            buff.append(' ');
        }
        return buff.toString();
    }
    
}
