/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;
import org.h2.mvstore.MVStore;
import org.h2.test.TestBase;
import org.h2.tools.SimpleResultSet;
import org.h2.tools.SimpleRowSource;
import org.h2.value.DataType;
import org.h2.value.ValueGeometry;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/**
 * Spatial datatype and index tests.
 * 
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
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
//            testSpatialValues();
//            testOverlap();
//            testNotOverlap();
//            testPersistentSpatialIndex();
//            testSpatialIndexQueryMultipleTable();
//            testIndexTransaction();
//            testJavaAlias();
//            testJavaAliasTableFunction();
            // testPersistentSpatialIndex2();
            testMemorySpatialIndex();
            testRandom();
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

    /**
     * Generate a random linestring under the given bounding box
     * @param minX Bounding box min x
     * @param maxX Bounding box max x
     * @param minY Bounding box min y
     * @param maxY Bounding box max y
     * @param maxLength LineString maximum length
     * @return A segment within this bounding box
     */
    public static Geometry getRandomGeometry(Random geometryRand,double minX,double maxX,double minY, double maxY, double maxLength) {
        GeometryFactory factory = new GeometryFactory();
        // Create the start point
        Coordinate start = new Coordinate(geometryRand.nextDouble()*(maxX-minX)+minX,
                geometryRand.nextDouble()*(maxY-minY)+minY);
        // Compute an angle
        double angle = geometryRand.nextDouble() * Math.PI * 2;
        // Compute length
        double length = geometryRand.nextDouble() * maxLength;
        // Compute end point
        Coordinate end = new Coordinate(start.x + Math.cos(angle) * length, start.y + Math.sin(angle) * length);
        return factory.createLineString(new Coordinate[]{start,end});
    }

    private void testRandom() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection("spatial");
        testRandom(conn, 69, 3500);
        testRandom(conn, 44, 3500);
        conn.close();
    }
    
   private void testRandom(Connection conn, long seed,long size) throws SQLException {
       Statement stat = conn.createStatement();
       stat.execute("drop table if exists test");
       Random geometryRand = new Random(seed);
       // Generate a set of geometry
       // It is marked as random, but it generate always the same geometry set, given the same seed
       stat.execute("create memory table test(id long primary key auto_increment, poly geometry)");
       // Create segment generation bounding box
       Envelope bbox = ValueGeometry.get("POLYGON ((301804.1049793153 2251719.1222191923," +
               " 301804.1049793153 2254747.2888244865, 304646.87362918374 2254747.2888244865," +
               " 304646.87362918374 2251719.1222191923, 301804.1049793153 2251719.1222191923))")
               .getGeometry().getEnvelopeInternal();
       // Create overlap test bounding box
       String testBBoxString = "POLYGON ((302215.44416332216 2252748, 302215.44416332216 2253851.781225762," +
               " 303582.85796541866 2253851.781225762, 303582.85796541866 2252748.526908161," +
               " 302215.44416332216 2252748))";
       Envelope testBBox = ValueGeometry.get(testBBoxString).getGeometry().getEnvelopeInternal();

       PreparedStatement ps = conn.prepareStatement("insert into test(poly) values (?)");
       long overlapCount = 0;
       Set<Integer> overlaps = new HashSet<Integer>(680);
       for(int i=1;i<=size;i++) {
           Geometry geometry = getRandomGeometry(geometryRand,bbox.getMinX(),bbox.getMaxX(),bbox.getMinY(),bbox.getMaxY(),200);
           ps.setObject(1,geometry);
           ps.execute();
           ResultSet keys = ps.getGeneratedKeys();
           keys.next();
           if(geometry.getEnvelopeInternal().intersects(testBBox)) {
               overlapCount++;
               overlaps.add(keys.getInt(1));
           }
       }
       ps.close();
       // Create index
       stat.execute("create spatial index idx_test_poly on test(poly)");
       // Must find the same overlap count with index
       ps = conn.prepareStatement("select id from test where poly && ?::Geometry");
       ps.setString(1,testBBoxString);
       ResultSet rs = ps.executeQuery();
       long found = 0;
       while(rs.next()) {
           overlaps.remove(rs.getInt(1));
           found++;
       }
       // Index count must be the same as sequential count
       assertEquals(overlapCount,found);
       // Missing id still in overlaps map
       assertTrue(overlaps.isEmpty());
       stat.execute("drop table if exists test");
   }
    private void testOverlap() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection("spatial");
        try {
            Statement stat = conn.createStatement();
            stat.execute("create memory table test(id int primary key, poly geometry)");
            stat.execute("insert into test values(1, 'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
            stat.execute("insert into test values(2, 'POLYGON ((3 1, 3 2, 4 2, 3 1))')");
            stat.execute("insert into test values(3, 'POLYGON ((1 3, 1 4, 2 4, 1 3))')");

            ResultSet rs = stat.executeQuery("select * from test where poly && 'POINT (1.5 1.5)'::Geometry");
            assertTrue(rs.next());
            assertEquals(1,rs.getInt("id"));
            assertFalse(rs.next());
            stat.execute("drop table test");
        } finally {
            conn.close();
        }
    }
    private void testPersistentSpatialIndex() throws SQLException {
        deleteDb("spatial_pers");
        Connection conn = getConnection("spatial_pers");
        try {
            Statement stat = conn.createStatement();
            stat.execute("create table test(id int primary key, poly geometry)");
            stat.execute("insert into test values(1, 'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
            stat.execute("insert into test values(2, 'POLYGON ((3 1, 3 2, 4 2, 3 1))')");
            stat.execute("insert into test values(3, 'POLYGON ((1 3, 1 4, 2 4, 1 3))')");
            stat.execute("create spatial index on test(poly)");

            ResultSet rs = stat.executeQuery("select * from test where poly && 'POINT (1.5 1.5)'::Geometry");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertFalse(rs.next());
            rs.close();

            // Test with multiple operator
            rs = stat.executeQuery("select * from test where poly && 'POINT (1.5 1.5)'::Geometry AND poly && 'POINT (1.7 1.75)'::Geometry");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertFalse(rs.next());
            rs.close();
        } finally {
            // Close the database
            conn.close();
        }

        conn = getConnection("spatial_pers");
        try {
            Statement stat = conn.createStatement();
            ResultSet rs = stat.executeQuery("select * from test where poly && 'POINT (1.5 1.5)'::Geometry");
            assertTrue(rs.next());
            assertEquals(1,rs.getInt("id"));
            assertFalse(rs.next());
            stat.execute("drop table test");
        } finally {
            conn.close();
        }

    }
    private void testNotOverlap() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection("spatial");
        try {
            Statement stat = conn.createStatement();
            stat.execute("create memory table test(id int primary key, poly geometry)");
            stat.execute("insert into test values(1, 'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
            stat.execute("insert into test values(2, 'POLYGON ((3 1, 3 2, 4 2, 3 1))')");
            stat.execute("insert into test values(3, 'POLYGON ((1 3, 1 4, 2 4, 1 3))')");

            ResultSet rs = stat.executeQuery("select * from test where NOT poly && 'POINT (1.5 1.5)'::Geometry");
            assertTrue(rs.next());
            assertEquals(2,rs.getInt("id"));
            assertTrue(rs.next());
            assertEquals(3,rs.getInt("id"));
            assertFalse(rs.next());
            stat.execute("drop table test");
        } finally {
            conn.close();
        }
    }

    private static void createTestTable(Statement stat)  throws SQLException {
        stat.execute("create table area(idarea int primary key, the_geom geometry)");
        stat.execute("create spatial index on area(the_geom)");
        stat.execute("insert into area values(1, 'POLYGON ((-10 109, 90 109, 90 9, -10 9, -10 109))')");
        stat.execute("insert into area values(2, 'POLYGON ((90 109, 190 109, 190 9, 90 9, 90 109))')");
        stat.execute("insert into area values(3, 'POLYGON ((190 109, 290 109, 290 9, 190 9, 190 109))')");
        stat.execute("insert into area values(4, 'POLYGON ((-10 9, 90 9, 90 -91, -10 -91, -10 9))')");
        stat.execute("insert into area values(5, 'POLYGON ((90 9, 190 9, 190 -91, 90 -91, 90 9))')");
        stat.execute("insert into area values(6, 'POLYGON ((190 9, 290 9, 290 -91, 190 -91, 190 9))')");
        stat.execute("create table roads(idroad int primary key, the_geom geometry)");
        stat.execute("create spatial index on roads(the_geom)");
        stat.execute("insert into roads values(1, 'LINESTRING (27.65595463138 -16.728733459357244, 47.61814744801515 40.435727788279806)')");
        stat.execute("insert into roads values(2, 'LINESTRING (17.674858223062415 55.861058601134246, 55.78449905482046 76.73062381852554)')");
        stat.execute("insert into roads values(3, 'LINESTRING (68.48771266540646 67.65689981096412, 108.4120982986768 88.52646502835542)')");
        stat.execute("insert into roads values(4, 'LINESTRING (177.3724007561437 18.65879017013235, 196.4272211720227 -16.728733459357244)')");
        stat.execute("insert into roads values(5, 'LINESTRING (106.5973534971645 -12.191871455576518, 143.79962192816637 30.454631379962223)')");
        stat.execute("insert into roads values(6, 'LINESTRING (144.70699432892252 55.861058601134246, 150.1512287334594 83.9896030245747)')");
        stat.execute("insert into roads values(7, 'LINESTRING (60.321361058601155 -13.099243856332663, 149.24385633270325 5.955576559546344)')");
    }

    private void testSpatialIndexQueryMultipleTable() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection("spatial");
        try {
            Statement stat = conn.createStatement();
            createTestTable(stat);
            testRoadAndArea(stat);
        } finally {
            // Close the database
            conn.close();
        }
        deleteDb("spatial");
    }
    private void testRoadAndArea(Statement stat) throws SQLException {
        ResultSet rs = stat.executeQuery("select idarea, COUNT(idroad) roadscount from area,roads where area.the_geom && roads.the_geom GROUP BY idarea ORDER BY idarea");
        assertTrue(rs.next());
        assertEquals(1,rs.getInt("idarea"));
        assertEquals(3,rs.getInt("roadscount"));
        assertTrue(rs.next());
        assertEquals(2,rs.getInt("idarea"));
        assertEquals(4,rs.getInt("roadscount"));
        assertTrue(rs.next());
        assertEquals(3,rs.getInt("idarea"));
        assertEquals(1,rs.getInt("roadscount"));
        assertTrue(rs.next());
        assertEquals(4,rs.getInt("idarea"));
        assertEquals(2,rs.getInt("roadscount"));
        assertTrue(rs.next());
        assertEquals(5,rs.getInt("idarea"));
        assertEquals(3,rs.getInt("roadscount"));
        assertTrue(rs.next());
        assertEquals(6,rs.getInt("idarea"));
        assertEquals(1,rs.getInt("roadscount"));
        assertFalse(rs.next());
        rs.close();
    }
    private void testIndexTransaction() throws SQLException {
        // Check session management in index
        deleteDb("spatialIndex");
        Connection conn = getConnection("spatialIndex");
        conn.setAutoCommit(false);
        try {
            Statement stat = conn.createStatement();
            createTestTable(stat);
            Savepoint sp = conn.setSavepoint();
            // Remove a row but do not commit
            stat.execute("delete from roads where idroad=7");
            // Check if index is updated
            ResultSet rs = stat.executeQuery("select idarea, COUNT(idroad) roadscount from area,roads where area.the_geom && roads.the_geom GROUP BY idarea ORDER BY idarea");
            assertTrue(rs.next());
            assertEquals(1,rs.getInt("idarea"));
            assertEquals(3,rs.getInt("roadscount"));
            assertTrue(rs.next());
            assertEquals(2,rs.getInt("idarea"));
            assertEquals(4,rs.getInt("roadscount"));
            assertTrue(rs.next());
            assertEquals(3,rs.getInt("idarea"));
            assertEquals(1,rs.getInt("roadscount"));
            assertTrue(rs.next());
            assertEquals(4,rs.getInt("idarea"));
            assertEquals(1,rs.getInt("roadscount"));
            assertTrue(rs.next());
            assertEquals(5,rs.getInt("idarea"));
            assertEquals(2,rs.getInt("roadscount"));
            assertTrue(rs.next());
            assertEquals(6,rs.getInt("idarea"));
            assertEquals(1,rs.getInt("roadscount"));
            assertFalse(rs.next());
            rs.close();
            conn.rollback(sp);
            // Check if the index is restored
            testRoadAndArea(stat);
        } finally {
            conn.close();
        }

    }
    
    /** 
     * Test the in the in-memory spatial index 
      */
    private void testMemorySpatialIndex() throws SQLException {
        deleteDb("spatialIndex");
        Connection conn = getConnection("spatialIndex");
        Statement stat = conn.createStatement();

        stat.execute("create memory table test(id int primary key, polygon geometry)");
        stat.execute("create spatial index idx_test_polygon on test(polygon)");
        stat.execute("insert into test values(1, 'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
        ResultSet rs;
        
        // an query that can not possibly return a result
        rs = stat.executeQuery("select * from test " + 
                "where polygon && 'POLYGON ((1 1, 1 2, 2 2, 1 1))'::Geometry " +
                "and polygon && 'POLYGON ((10 10, 10 20, 20 20, 10 10))'::Geometry");
        assertFalse(rs.next());
        
        rs = stat.executeQuery("explain select * from test where polygon && 'POLYGON ((1 1, 1 2, 2 2, 1 1))'::Geometry");
        rs.next();
        assertContains(rs.getString(1), "/* PUBLIC.IDX_TEST_POLYGON: POLYGON &&");

        int todo;
        // TODO equality should probably also use the spatial index
        // rs = stat.executeQuery("explain select * from test where polygon = 'POLYGON ((1 1, 1 2, 2 2, 1 1))'");
        // rs.next();
        // assertContains(rs.getString(1), "/* PUBLIC.IDX_TEST_POLYGON: POLYGON =");

        // these queries actually have no meaning in the context of a spatial index, but 
        // check them anyhow
        stat.executeQuery("select * from test where polygon > 'POLYGON ((1 1, 1 2, 2 2, 1 1))'::Geometry");
        stat.executeQuery("select * from test where polygon < 'POLYGON ((1 1, 1 2, 2 2, 1 1))'::Geometry");

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

    /**
     * Test java alias with Geometry type.
     */
    private void testJavaAlias() throws SQLException {
        deleteDb("spatialIndex");
        Connection conn = getConnection("spatialIndex");
        try {
            Statement stat = conn.createStatement();
            stat.execute("CREATE ALIAS T_GEOMFROMTEXT FOR \"" + TestSpatial.class.getName() + ".geomFromText\"");
            stat.execute("create table test(id int primary key auto_increment, the_geom geometry)");
            stat.execute("insert into test(the_geom) values(T_GEOMFROMTEXT('POLYGON ((62 48, 84 48, 84 42, 56 34, 62 48))',1488))");
            stat.execute("DROP ALIAS T_GEOMFROMTEXT");
            ResultSet rs = stat.executeQuery("select the_geom from test");
            assertTrue(rs.next());
            assertEquals("POLYGON ((62 48, 84 48, 84 42, 56 34, 62 48))", rs.getObject(1).toString());
        } finally {
            conn.close();
        }
        deleteDb("spatialIndex");
    }

    /**
     * Test java alias with Geometry type.
     */
    private void testJavaAliasTableFunction() throws SQLException {
        deleteDb("spatialIndex");
        Connection conn = getConnection("spatialIndex");
        try {
            Statement stat = conn.createStatement();
            stat.execute("CREATE ALIAS T_RANDOM_GEOM_TABLE FOR \"" + TestSpatial.class.getName() + ".getRandomGeometryTable\"");
            stat.execute("create table test as select * from T_RANDOM_GEOM_TABLE(42,20,-100,100,-100,100,4)");
            stat.execute("DROP ALIAS T_RANDOM_GEOM_TABLE");
            ResultSet rs = stat.executeQuery("select count(*) cpt from test");
            assertTrue(rs.next());
            assertEquals(20, rs.getInt(1));
        } finally {
            conn.close();
        }
        deleteDb("spatialIndex");
    }


    public static ResultSet getRandomGeometryTable(final long seed,final long rowCount, final double minX,final double maxX,final double minY, final double maxY, final double maxLength) {
        SimpleResultSet rs = new SimpleResultSet(new SimpleRowSource() {
            private final Random rnd = new Random(seed);
            private int cpt = 0;
            @Override
            public Object[] readRow() throws SQLException {
                if(cpt++<rowCount) {
                    return new Object[]{getRandomGeometry(rnd,minX,maxX,minY,maxY,maxLength)};  //To change body of implemented methods use File | Settings | File Templates.
                } else {
                    return null;
                }
            }

            @Override
            public void close() {
            }

            @Override
            public void reset() throws SQLException {
                rnd.setSeed(seed);
            }
        });
        rs.addColumn("the_geom", Types.OTHER,Integer.MAX_VALUE,0);
        return rs;
    }

    /**
     *
     * @param text Geometry in Well Known Text
     * @param srid Projection ID
     * @return Geometry object
     */
    public static Geometry geomFromText(String text, int srid) throws SQLException {
        WKTReader wktReader = new WKTReader();
        try {
            Geometry geom = wktReader.read(text);
            geom.setSRID(srid);
            return geom;
        } catch (ParseException ex) {
            throw new SQLException(ex);
        }
    }
    /**
     * Not really a test case but show that something go crazy (in mvstore) after some seconds.
     * @throws SQLException
     */
    private void testPersistentSpatialIndex2() throws SQLException {
        deleteDb("spatial_pers");
        final long count = 150000;
        Connection conn = getConnection("spatial_pers");
        try {
            Statement stat = conn.createStatement();
            stat.execute("create table test(id int primary key auto_increment, the_geom geometry)");
            PreparedStatement ps = conn.prepareStatement("insert into test(the_geom) values(?)");
            Random rnd = new Random(44);
            for(int i=0;i<count;i++) {
                ps.setObject(1,getRandomGeometry(rnd,0,100,-50,50,3));
                ps.execute();
            }
            stat.execute("create spatial index on test(the_geom)");
            Database db = ((Session)((JdbcConnection) conn).getSession()).getDatabase();
            MVStore store = db.getMvStore().getStore();
            int cpt=0;
            while(cpt<46) {
                try {
                    // First it shows 610, then 5 until cpt==44, finally at cpt==45 it shows an unsaved 688 with a trace in spatial_pers.trace.db
                    System.out.println((cpt++)+" store.getUnsavedPageCount()=="+store.getUnsavedPageCount());
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    throw new SQLException(ex);
                }
            }
        } finally {
            // Close the database
            conn.close();
        }
    }
    
}
