/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.test.TestBase;
import org.h2.value.DataType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

/**
 * Spatial datatype and index tests.
 */
public class TestSpatial extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException {
        if (DataType.GEOMETRY_CLASS != null) {
            deleteDb("spatial");
            testSpatialValues();
            testMemorySpatialIndex();
            deleteDb("spatial");
        }
    }

    private void testSpatialValues() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection("spatial");
        Statement stat = conn.createStatement();

        stat.execute("create memory table test(id int primary key, polygon geometry)");
        stat.execute("insert into test values(1, 'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
        ResultSet rs = stat.executeQuery("select * from test");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("POLYGON ((1 1, 1 2, 2 2, 1 1))", rs.getString(2));
        GeometryFactory f = new GeometryFactory();
        Polygon polygon = f.createPolygon(new Coordinate[] {
                new Coordinate(1, 1),
                new Coordinate(1, 2),
                new Coordinate(2, 2),
                new Coordinate(1, 1) });
        assertTrue(polygon.equals(rs.getObject(2)));

        rs = stat.executeQuery("select * from test where polygon = 'POLYGON ((1 1, 1 2, 2 2, 1 1))'");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        stat.executeQuery("select * from test where polygon > 'POLYGON ((1 1, 1 2, 2 2, 1 1))'");
        stat.executeQuery("select * from test where polygon < 'POLYGON ((1 1, 1 2, 2 2, 1 1))'");

        stat.execute("drop table test");
        conn.close();
        deleteDb("spatial");
    }

    /** test in the in-memory spatial index */
    private void testMemorySpatialIndex() throws SQLException {
        deleteDb("spatialIndex");
        Connection conn = getConnection("spatialIndex");
        Statement stat = conn.createStatement();

        stat.execute("create memory table test(id int primary key, polygon geometry)");
        stat.execute("create spatial index idx_test_polygon on test(polygon)");
        stat.execute("insert into test values(1, 'POLYGON ((1 1, 1 2, 2 2, 1 1))')");

        ResultSet rs = stat.executeQuery("explain select * from test where polygon = 'POLYGON ((1 1, 1 2, 2 2, 1 1))'");
        rs.next();
        assertContains(rs.getString(1), "/* PUBLIC.IDX_TEST_POLYGON: POLYGON =");

        // these queries actually have no meaning in the context of a spatial index,
        // just check that the query works
        stat.executeQuery("select * from test where polygon = 'POLYGON ((1 1, 1 2, 2 2, 1 1))'");
        stat.executeQuery("select * from test where polygon > 'POLYGON ((1 1, 1 2, 2 2, 1 1))'");
        stat.executeQuery("select * from test where polygon < 'POLYGON ((1 1, 1 2, 2 2, 1 1))'");

        rs = stat.executeQuery("select * from test where intersects(polygon, 'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
        assertTrue(rs.next());

        rs = stat.executeQuery("select * from test where intersects(polygon, 'POINT (1 1)')");
        assertTrue(rs.next());

        rs = stat.executeQuery("select * from test where intersects(polygon, 'POINT (0 0)')");
        assertFalse(rs.next());

        stat.execute("drop table test");
        conn.close();
        deleteDb("spatialIndex");
    }

}
