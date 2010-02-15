/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.h2.test.TestBase;
import org.h2.tools.MultiDimension;

/**
 * Tests the multi-dimension index tool.
 */
public class TestMultiDimension extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        Random rand = new Random(10);
        for (int i = 0; i < 1000; i++) {
            int x = rand.nextInt(1000), y = rand.nextInt(1000), z = rand.nextInt(1000);
            MultiDimension tool = MultiDimension.getInstance();
            long xyz = tool.interleave(new int[] { x, y, z });
            assertEquals(x, tool.deinterleave(xyz, 3, 0));
            assertEquals(y, tool.deinterleave(xyz, 3, 1));
            assertEquals(z, tool.deinterleave(xyz, 3, 2));
        }

        deleteDb("multiDimension");
        Connection conn;
        conn = getConnection("multiDimension");
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS MAP FOR \"" + getClass().getName() + ".interleave\"");
        stat.execute("CREATE TABLE TEST(X INT NOT NULL, Y INT NOT NULL, Z INT NOT NULL, " +
                "XYZ BIGINT AS MAP(X, Y, Z), DATA VARCHAR)");
        stat.execute("CREATE INDEX IDX_X ON TEST(X, Y, Z)");
        stat.execute("CREATE INDEX IDX_XYZ ON TEST(XYZ)");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST(X, Y, Z, DATA) VALUES(?, ?, ?, ?)");
        // a reasonable max value to see the performance difference is 60; the
        // higher the bigger the difference
        int max = getSize(10, 20);
        long time = System.currentTimeMillis();
        for (int x = 0; x < max; x++) {
            for (int y = 0; y < max; y++) {
                for (int z = 0; z < max; z++) {
                    long t2 = System.currentTimeMillis();
                    if (t2 - time > 1000) {
                        int percent = (int) (100.0 * ((double) x * x * x) / ((double) max * max * max));
                        trace(percent + "%");
                        time = t2;
                        try {
                            Thread.sleep(10);
                        } catch (Exception e) {
                            // ignore
                        }
                    }
                    prep.setInt(1, x);
                    prep.setInt(2, y);
                    prep.setInt(3, z);
                    prep.setString(4, "Test data");
                    prep.execute();
                }
            }
        }
        stat.execute("ANALYZE SAMPLE_SIZE 10000");
        PreparedStatement prepRegular = conn.prepareStatement(
                "SELECT * FROM TEST WHERE X BETWEEN ? AND ? " +
                "AND Y BETWEEN ? AND ? AND Z BETWEEN ? AND ? ORDER BY X, Y, Z");
        MultiDimension multi = MultiDimension.getInstance();
        String sql = multi.generatePreparedQuery("TEST", "XYZ", new String[] { "X", "Y", "Z" });
        sql += " ORDER BY X, Y, Z";
        PreparedStatement prepMulti = conn.prepareStatement(sql);
        long timeMulti = 0, timeRegular = 0;
        int timeMax = getSize(100, 2000);
        for (int i = 0; timeMulti < timeMax; i++) {
            int size = rand.nextInt(max / 10);
            int minX = rand.nextInt(max - size);
            int minY = rand.nextInt(max - size);
            int minZ = rand.nextInt(max - size);
            int maxX = minX + size, maxY = minY + size, maxZ = minZ + size;
            time = System.currentTimeMillis();
            ResultSet rs1 = multi.getResult(prepMulti, new int[] { minX, minY, minZ }, new int[] { maxX, maxY, maxZ });
            timeMulti += System.currentTimeMillis() - time;
            time = System.currentTimeMillis();
            prepRegular.setInt(1, minX);
            prepRegular.setInt(2, maxX);
            prepRegular.setInt(3, minY);
            prepRegular.setInt(4, maxY);
            prepRegular.setInt(5, minZ);
            prepRegular.setInt(6, maxZ);
            ResultSet rs2 = prepRegular.executeQuery();
            timeRegular += System.currentTimeMillis() - time;
            while (rs1.next()) {
                assertTrue(rs2.next());
                assertEquals(rs1.getInt(1), rs2.getInt(1));
                assertEquals(rs1.getInt(2), rs2.getInt(2));
            }
            assertFalse(rs2.next());
        }
        conn.close();
        deleteDb("multiDimension");
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param x the x value
     * @param y the y value
     * @param z the z value
     * @return the bit-interleaved value
     */
    public static long interleave(int x, int y, int z) {
        return MultiDimension.getInstance().interleave(new int[] { x, y, z });
    }
}
