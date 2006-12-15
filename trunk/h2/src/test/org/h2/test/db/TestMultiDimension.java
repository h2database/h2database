/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

import org.h2.test.TestBase;
import org.h2.tools.MultiDimension;

public class TestMultiDimension extends TestBase {

    public void test() throws Exception {
        Random rand = new Random(10);
        for(int i=0; i<1000; i++) {
            int x = rand.nextInt(1000), y = rand.nextInt(1000), z = rand.nextInt(1000);
            MultiDimension tool = MultiDimension.getInstance();
            long xyz = tool.interleave(new int[]{x, y, z});
            check(x, tool.deinterleave(xyz, 3, 0));
            check(y, tool.deinterleave(xyz, 3, 1));
            check(z, tool.deinterleave(xyz, 3, 2));
        }
        
        deleteDb("multiDimension");
        Connection conn;
        conn = getConnection("multiDimension");
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS MAP FOR \"" + getClass().getName() + ".interleave\"");
        stat.execute("CREATE TABLE TEST(X INT NOT NULL, Y INT NOT NULL, Z INT NOT NULL, XYZ BIGINT AS MAP(X, Y, Z), DATA VARCHAR)");
        stat.execute("CREATE INDEX IDX_X ON TEST(X, Y, Z)");
        stat.execute("CREATE INDEX IDX_XYZ ON TEST(XYZ)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(X, Y, Z, DATA) VALUES(?, ?, ?, ?)");
        
        // a reasonable value to see the performance difference is 60
        int max = 10;
        long time = System.currentTimeMillis();
        for(int x=0; x<max; x++) {
            for(int y=0; y<max; y++) {
                for(int z=0; z<max; z++) {
                    long t2 = System.currentTimeMillis();
                    if(t2 - time > 1000) {
                        int percent = (int)(100.0 * ((double)x*x*x) / ((double)max * max * max));
                        trace(percent + "%");
                        time = t2;
                        try {
                            Thread.sleep(10);
                        } catch(Exception e) {}
                    }
                    prep.setInt(1, x);
                    prep.setInt(2, y);
                    prep.setInt(3, z);
                    prep.setString(4, "Test data");
                    prep.execute();
                }
            }
        }
        for(int i=0; i<50; i++) {
            int size = rand.nextInt(max / 10);
            int minX = rand.nextInt(max-size);
            int minY = rand.nextInt(max-size);
            int minZ = rand.nextInt(max-size);
            int maxX = minX+size, maxY = minY+size, maxZ = minZ+size;
            
            long time1 = System.currentTimeMillis();
            String query1 = MultiDimension.getInstance().getMultiDimensionalQuery(
                        "TEST", "XYZ",
                        new String[]{"X", "Y", "Z"}, 
                        new int[]{minX, minY, minZ}, 
                        new int[]{minX+size, minY+size, minZ+size});
            ResultSet rs1 = conn.createStatement().executeQuery(query1 + " ORDER BY X, Y, Z");
            time1 = System.currentTimeMillis() - time1;
            
            long time2 = System.currentTimeMillis();
            String query2 = "SELECT * FROM TEST WHERE " 
                +"X BETWEEN " + minX + " AND " + maxX + " AND "
                +"Y BETWEEN " + minY + " AND " + maxY + " AND "
                +"Z BETWEEN " + minZ + " AND " + maxZ;
            
            PreparedStatement prep2 = conn.prepareStatement(query2 + " ORDER BY X, Y, Z");
            ResultSet rs2 = prep2.executeQuery();
            time2 = System.currentTimeMillis() - time2;
            while(rs1.next()) {
                check(rs2.next());
                check(rs1.getInt(1), rs2.getInt(1));
                check(rs1.getInt(2), rs2.getInt(2));
            }
            checkFalse(rs2.next());
            trace("t1="+time1+" t2="+time2+" size="+size);
        }
        conn.close();
    }
    
    public static long interleave(int x, int y, int z) {
        return MultiDimension.getInstance().interleave(new int[]{x, y, z});
    }
}
