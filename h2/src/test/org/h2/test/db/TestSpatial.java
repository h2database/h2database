/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Types;
import java.util.Random;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.util.AffineTransformation;
import org.h2.api.Aggregate;
import org.h2.test.TestBase;
import org.h2.tools.SimpleResultSet;
import org.h2.tools.SimpleRowSource;
import org.h2.value.DataType;
import org.h2.value.Value;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.h2.value.ValueGeometry;

/**
 * Spatial datatype and index tests.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class TestSpatial extends TestBase {

    private String url = "spatial";

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
        if (!config.mvStore && config.mvcc) {
            return;
        }
        if (config.memory && config.mvcc) {
            return;
        }
        if (DataType.GEOMETRY_CLASS != null) {
            deleteDb("spatial");
            url = "spatial";
            testSpatial();
            deleteDb("spatial");
        }
    }

    private void testSpatial() throws SQLException {
        testSpatialValues();
        testOverlap();
        testNotOverlap();
        testPersistentSpatialIndex();
        testSpatialIndexQueryMultipleTable();
        testIndexTransaction();
        testJavaAlias();
        testJavaAliasTableFunction();
        testMemorySpatialIndex();
        testGeometryDataType();
        testWKB();
        testValueConversion();
        testEquals();
        testTableFunctionGeometry();
        testHashCode();
        testAggregateWithGeometry();
        testTableViewSpatialPredicate();
        testValueGeometryScript();
        testInPlaceUpdate();
    }

    private void testHashCode() {
        ValueGeometry geomA = ValueGeometry
                .get("POLYGON ((67 13 6, 67 18 5, 59 18 4, 59 13 6,  67 13 6))");
        ValueGeometry geomB = ValueGeometry
                .get("POLYGON ((67 13 6, 67 18 5, 59 18 4, 59 13 6,  67 13 6))");
        ValueGeometry geomC = ValueGeometry
                .get("POLYGON ((67 13 6, 67 18 5, 59 18 4, 59 13 5,  67 13 6))");
        assertEquals(geomA.hashCode(), geomB.hashCode());
        assertFalse(geomA.hashCode() == geomC.hashCode());
    }

    private void testSpatialValues() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection(url);
        Statement stat = conn.createStatement();

        stat.execute("create memory table test" +
                "(id int primary key, polygon geometry)");
        stat.execute("insert into test values(1, " +
                "'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
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

        rs = stat.executeQuery("select * from test where polygon = " +
                "'POLYGON ((1 1, 1 2, 2 2, 1 1))'");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        stat.executeQuery("select * from test where polygon > " +
                "'POLYGON ((1 1, 1 2, 2 2, 1 1))'");
        stat.executeQuery("select * from test where polygon < " +
                "'POLYGON ((1 1, 1 2, 2 2, 1 1))'");

        stat.execute("drop table test");
        conn.close();
        deleteDb("spatial");
    }

    /**
     * Generate a random line string under the given bounding box.
     *
     * @param geometryRand the random generator
     * @param minX Bounding box min x
     * @param maxX Bounding box max x
     * @param minY Bounding box min y
     * @param maxY Bounding box max y
     * @param maxLength LineString maximum length
     * @return A segment within this bounding box
     */
    static Geometry getRandomGeometry(Random geometryRand,
            double minX, double maxX,
            double minY, double maxY, double maxLength) {
        GeometryFactory factory = new GeometryFactory();
        // Create the start point
        Coordinate start = new Coordinate(
                geometryRand.nextDouble() * (maxX - minX) + minX,
                geometryRand.nextDouble() * (maxY - minY) + minY);
        // Compute an angle
        double angle = geometryRand.nextDouble() * Math.PI * 2;
        // Compute length
        double length = geometryRand.nextDouble() * maxLength;
        // Compute end point
        Coordinate end = new Coordinate(
                start.x + Math.cos(angle) * length,
                start.y + Math.sin(angle) * length);
        return factory.createLineString(new Coordinate[] { start, end });
    }

    private void testOverlap() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection(url);
        try {
            Statement stat = conn.createStatement();
            stat.execute("create memory table test" +
                    "(id int primary key, poly geometry)");
            stat.execute("insert into test values(1, " +
                    "'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
            stat.execute("insert into test values(2, " +
                    "'POLYGON ((3 1, 3 2, 4 2, 3 1))')");
            stat.execute("insert into test values(3, " +
                    "'POLYGON ((1 3, 1 4, 2 4, 1 3))')");

            ResultSet rs = stat.executeQuery(
                    "select * from test " +
                    "where poly && 'POINT (1.5 1.5)'::Geometry");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertFalse(rs.next());
            stat.execute("drop table test");
        } finally {
            conn.close();
        }
    }
    private void testPersistentSpatialIndex() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection(url);
        try {
            Statement stat = conn.createStatement();
            stat.execute("create table test" +
                    "(id int primary key, poly geometry)");
            stat.execute("insert into test values(1, " +
                    "'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
            stat.execute("insert into test values(2, " +
                    "'POLYGON ((3 1, 3 2, 4 2, 3 1))')");
            stat.execute("insert into test values(3, " +
                    "'POLYGON ((1 3, 1 4, 2 4, 1 3))')");
            stat.execute("create spatial index on test(poly)");

            ResultSet rs = stat.executeQuery(
                    "select * from test " +
                    "where poly && 'POINT (1.5 1.5)'::Geometry");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertFalse(rs.next());
            rs.close();

            // Test with multiple operator
            rs = stat.executeQuery(
                    "select * from test " +
                    "where poly && 'POINT (1.5 1.5)'::Geometry " +
                    "AND poly && 'POINT (1.7 1.75)'::Geometry");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertFalse(rs.next());
            rs.close();
        } finally {
            // Close the database
            conn.close();
        }

        if (config.memory) {
            return;
        }

        conn = getConnection(url);
        try {
            Statement stat = conn.createStatement();
            ResultSet rs = stat.executeQuery(
                    "select * from test " +
                    "where poly && 'POINT (1.5 1.5)'::Geometry");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertFalse(rs.next());
            stat.execute("drop table test");
        } finally {
            conn.close();
        }

    }
    private void testNotOverlap() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection(url);
        try {
            Statement stat = conn.createStatement();
            stat.execute("create memory table test" +
                    "(id int primary key, poly geometry)");
            stat.execute("insert into test values(1, " +
                    "'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
            stat.execute("insert into test values(2, " +
                    "'POLYGON ((3 1, 3 2, 4 2, 3 1))')");
            stat.execute("insert into test values(3, " +
                    "'POLYGON ((1 3, 1 4, 2 4, 1 3))')");

            ResultSet rs = stat.executeQuery(
                    "select * from test " +
                    "where NOT poly && 'POINT (1.5 1.5)'::Geometry");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertFalse(rs.next());
            stat.execute("drop table test");
        } finally {
            conn.close();
        }
    }

    private static void createTestTable(Statement stat)  throws SQLException {
        stat.execute("create table area(idArea int primary key, the_geom geometry)");
        stat.execute("create spatial index on area(the_geom)");
        stat.execute("insert into area values(1, " +
                "'POLYGON ((-10 109, 90 109, 90 9, -10 9, -10 109))')");
        stat.execute("insert into area values(2, " +
                "'POLYGON ((90 109, 190 109, 190 9, 90 9, 90 109))')");
        stat.execute("insert into area values(3, " +
                "'POLYGON ((190 109, 290 109, 290 9, 190 9, 190 109))')");
        stat.execute("insert into area values(4, " +
                "'POLYGON ((-10 9, 90 9, 90 -91, -10 -91, -10 9))')");
        stat.execute("insert into area values(5, " +
                "'POLYGON ((90 9, 190 9, 190 -91, 90 -91, 90 9))')");
        stat.execute("insert into area values(6, " +
                "'POLYGON ((190 9, 290 9, 290 -91, 190 -91, 190 9))')");
        stat.execute("create table roads(idRoad int primary key, the_geom geometry)");
        stat.execute("create spatial index on roads(the_geom)");
        stat.execute("insert into roads values(1, " +
                "'LINESTRING (27.65595463138 -16.728733459357244, " +
                "47.61814744801515 40.435727788279806)')");
        stat.execute("insert into roads values(2, " +
                "'LINESTRING (17.674858223062415 55.861058601134246, " +
                "55.78449905482046 76.73062381852554)')");
        stat.execute("insert into roads values(3, " +
                "'LINESTRING (68.48771266540646 67.65689981096412, " +
                "108.4120982986768 88.52646502835542)')");
        stat.execute("insert into roads values(4, " +
                "'LINESTRING (177.3724007561437 18.65879017013235, " +
                "196.4272211720227 -16.728733459357244)')");
        stat.execute("insert into roads values(5, " +
                "'LINESTRING (106.5973534971645 -12.191871455576518, " +
                "143.79962192816637 30.454631379962223)')");
        stat.execute("insert into roads values(6, " +
                "'LINESTRING (144.70699432892252 55.861058601134246, " +
                "150.1512287334594 83.9896030245747)')");
        stat.execute("insert into roads values(7, " +
                "'LINESTRING (60.321361058601155 -13.099243856332663, " +
                "149.24385633270325 5.955576559546344)')");
    }

    private void testSpatialIndexQueryMultipleTable() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection(url);
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
        ResultSet rs = stat.executeQuery(
                "select idArea, COUNT(idRoad) roadCount " +
                "from area, roads " +
                "where area.the_geom && roads.the_geom " +
                "GROUP BY idArea ORDER BY idArea");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("idArea"));
        assertEquals(3, rs.getInt("roadCount"));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("idArea"));
        assertEquals(4, rs.getInt("roadCount"));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("idArea"));
        assertEquals(1, rs.getInt("roadCount"));
        assertTrue(rs.next());
        assertEquals(4, rs.getInt("idArea"));
        assertEquals(2, rs.getInt("roadCount"));
        assertTrue(rs.next());
        assertEquals(5, rs.getInt("idArea"));
        assertEquals(3, rs.getInt("roadCount"));
        assertTrue(rs.next());
        assertEquals(6, rs.getInt("idArea"));
        assertEquals(1, rs.getInt("roadCount"));
        assertFalse(rs.next());
        rs.close();
    }
    private void testIndexTransaction() throws SQLException {
        // Check session management in index
        deleteDb("spatial");
        Connection conn = getConnection(url);
        conn.setAutoCommit(false);
        try {
            Statement stat = conn.createStatement();
            createTestTable(stat);
            Savepoint sp = conn.setSavepoint();
            // Remove a row but do not commit
            stat.execute("delete from roads where idRoad=7");
            // Check if index is updated
            ResultSet rs = stat.executeQuery(
                    "select idArea, COUNT(idRoad) roadCount " +
                    "from area, roads " +
                    "where area.the_geom && roads.the_geom " +
                    "GROUP BY idArea ORDER BY idArea");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("idArea"));
            assertEquals(3, rs.getInt("roadCount"));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("idArea"));
            assertEquals(4, rs.getInt("roadCount"));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("idArea"));
            assertEquals(1, rs.getInt("roadCount"));
            assertTrue(rs.next());
            assertEquals(4, rs.getInt("idArea"));
            assertEquals(1, rs.getInt("roadCount"));
            assertTrue(rs.next());
            assertEquals(5, rs.getInt("idArea"));
            assertEquals(2, rs.getInt("roadCount"));
            assertTrue(rs.next());
            assertEquals(6, rs.getInt("idArea"));
            assertEquals(1, rs.getInt("roadCount"));
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
        deleteDb("spatial");
        Connection conn = getConnection(url);
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

        rs = stat.executeQuery(
                "explain select * from test " +
                "where polygon && 'POLYGON ((1 1, 1 2, 2 2, 1 1))'::Geometry");
        rs.next();
        assertContains(rs.getString(1), "/* PUBLIC.IDX_TEST_POLYGON: POLYGON &&");

        // TODO equality should probably also use the spatial index
        // rs = stat.executeQuery("explain select * from test " +
        //         "where polygon = 'POLYGON ((1 1, 1 2, 2 2, 1 1))'");
        // rs.next();
        // assertContains(rs.getString(1),
        //         "/* PUBLIC.IDX_TEST_POLYGON: POLYGON =");

        // these queries actually have no meaning in the context of a spatial
        // index, but
        // check them anyhow
        stat.executeQuery("select * from test where polygon > " +
                "'POLYGON ((1 1, 1 2, 2 2, 1 1))'::Geometry");
        stat.executeQuery("select * from test where polygon < " +
                "'POLYGON ((1 1, 1 2, 2 2, 1 1))'::Geometry");

        rs = stat.executeQuery(
                "select * from test " +
                "where intersects(polygon, 'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
        assertTrue(rs.next());

        rs = stat.executeQuery(
                "select * from test " +
                "where intersects(polygon, 'POINT (1 1)')");
        assertTrue(rs.next());

        rs = stat.executeQuery(
                "select * from test " +
                "where intersects(polygon, 'POINT (0 0)')");
        assertFalse(rs.next());

        stat.execute("drop table test");
        conn.close();
        deleteDb("spatial");
    }

    /**
     * Test java alias with Geometry type.
     */
    private void testJavaAlias() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection(url);
        try {
            Statement stat = conn.createStatement();
            stat.execute("CREATE ALIAS T_GEOM_FROM_TEXT FOR \"" +
                    TestSpatial.class.getName() + ".geomFromText\"");
            stat.execute("create table test(id int primary key " +
                    "auto_increment, the_geom geometry)");
            stat.execute("insert into test(the_geom) values(" +
                    "T_GEOM_FROM_TEXT('POLYGON ((" +
                    "62 48, 84 48, 84 42, 56 34, 62 48))',1488))");
            stat.execute("DROP ALIAS T_GEOM_FROM_TEXT");
            ResultSet rs = stat.executeQuery("select the_geom from test");
            assertTrue(rs.next());
            assertEquals("POLYGON ((62 48, 84 48, 84 42, 56 34, 62 48))",
                    rs.getObject(1).toString());
        } finally {
            conn.close();
        }
        deleteDb("spatial");
    }

    /**
     * Test java alias with Geometry type.
     */
    private void testJavaAliasTableFunction() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection(url);
        try {
            Statement stat = conn.createStatement();
            stat.execute("CREATE ALIAS T_RANDOM_GEOM_TABLE FOR \"" +
                    TestSpatial.class.getName() + ".getRandomGeometryTable\"");
            stat.execute(
                    "create table test as " +
                    "select * from T_RANDOM_GEOM_TABLE(42,20,-100,100,-100,100,4)");
            stat.execute("DROP ALIAS T_RANDOM_GEOM_TABLE");
            ResultSet rs = stat.executeQuery("select count(*) from test");
            assertTrue(rs.next());
            assertEquals(20, rs.getInt(1));
        } finally {
            conn.close();
        }
        deleteDb("spatial");
    }

    /**
     * Generate a result set with random geometry data.
     * Used as an ALIAS function.
     *
     * @param seed the random seed
     * @param rowCount the number of rows
     * @param minX the smallest x
     * @param maxX the largest x
     * @param minY the smallest y
     * @param maxY the largest y
     * @param maxLength the maximum length
     * @return a result set
     */
    public static ResultSet getRandomGeometryTable(
            final long seed, final long rowCount,
            final double minX, final double maxX,
            final double minY, final double maxY, final double maxLength) {

        SimpleResultSet rs = new SimpleResultSet(new SimpleRowSource() {

            private final Random random = new Random(seed);
            private int currentRow;

            @Override
            public Object[] readRow() throws SQLException {
                if (currentRow++ < rowCount) {
                    return new Object[] {
                            getRandomGeometry(random,
                                    minX, maxX, minY, maxY, maxLength) };
                }
                return null;
            }

            @Override
            public void close() {
                // nothing to do
            }

            @Override
            public void reset() throws SQLException {
                random.setSeed(seed);
            }
        });
        rs.addColumn("the_geom", Types.OTHER, "GEOMETRY", Integer.MAX_VALUE, 0);
        return rs;
    }

    /**
     * Convert the text to a geometry object.
     *
     * @param text the geometry as a Well Known Text
     * @param srid the projection id
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

    private void testGeometryDataType() {
        GeometryFactory geometryFactory = new GeometryFactory();
        Geometry geometry = geometryFactory.createPoint(new Coordinate(0, 0));
        assertEquals(Value.GEOMETRY, DataType.getTypeFromClass(geometry.getClass()));
    }

    /**
     * Test serialization of Z and SRID values.
     */
    private void testWKB() {
        ValueGeometry geom3d = ValueGeometry.get(
                "POLYGON ((67 13 6, 67 18 5, 59 18 4, 59 13 6,  67 13 6))", 27572);
        ValueGeometry copy = ValueGeometry.get(geom3d.getBytes());
        assertEquals(6, copy.getGeometry().getCoordinates()[0].z);
        assertEquals(5, copy.getGeometry().getCoordinates()[1].z);
        assertEquals(4, copy.getGeometry().getCoordinates()[2].z);
        // Test SRID
        copy = ValueGeometry.get(geom3d.getBytes());
        assertEquals(27572, copy.getGeometry().getSRID());
    }

    /**
     * Test conversion of Geometry object into Object
     */
    private void testValueConversion() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection(url);
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS OBJ_STRING FOR \"" +
                TestSpatial.class.getName() +
                ".getObjectString\"");
        ResultSet rs = stat.executeQuery(
                "select OBJ_STRING('POINT( 15 25 )'::geometry)");
        assertTrue(rs.next());
        assertEquals("POINT (15 25)", rs.getString(1));
        conn.close();
        deleteDb("spatial");
    }

    /**
     * Get the toString value of the object.
     *
     * @param object the object
     * @return the string representation
     */
    public static String getObjectString(Object object) {
        return object.toString();
    }

    /**
     * Test equality method on ValueGeometry
     */
    private void testEquals() {
        // 3d equality test
        ValueGeometry geom3d = ValueGeometry.get(
                "POLYGON ((67 13 6, 67 18 5, 59 18 4, 59 13 6,  67 13 6))");
        ValueGeometry geom2d = ValueGeometry.get(
                "POLYGON ((67 13, 67 18, 59 18, 59 13,  67 13))");
        assertFalse(geom3d.equals(geom2d));
        // SRID equality test
        GeometryFactory geometryFactory = new GeometryFactory();
        Geometry geometry = geometryFactory.createPoint(new Coordinate(0, 0));
        geometry.setSRID(27572);
        ValueGeometry valueGeometry =
                ValueGeometry.getFromGeometry(geometry);
        Geometry geometry2 = geometryFactory.createPoint(new Coordinate(0, 0));
        geometry2.setSRID(5326);
        ValueGeometry valueGeometry2 =
                ValueGeometry.getFromGeometry(geometry2);
        assertFalse(valueGeometry.equals(valueGeometry2));
        // Check illegal geometry (no WKB representation)
        try {
            ValueGeometry.get("POINT EMPTY");
            fail("expected this to throw IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected
        }
    }

    /**
     * Check that geometry column type is kept with a table function
     */
    private void testTableFunctionGeometry() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection(url);
        try {
            Statement stat = conn.createStatement();
            stat.execute("CREATE ALIAS POINT_TABLE FOR \"" +
                    TestSpatial.class.getName() + ".pointTable\"");
            stat.execute("create table test as select * from point_table(1, 1)");
            // Read column type
            ResultSet columnMeta = conn.getMetaData().
                    getColumns(null, null, "TEST", "THE_GEOM");
            assertTrue(columnMeta.next());
            assertEquals("geometry",
                    columnMeta.getString("TYPE_NAME").toLowerCase());
            assertFalse(columnMeta.next());
        } finally {
            conn.close();
        }
        deleteDb("spatial");
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param x the x position of the point
     * @param y the y position of the point
     * @return a result set with this point
     */
    public static ResultSet pointTable(double x, double y) {
        GeometryFactory factory = new GeometryFactory();
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("THE_GEOM", Types.JAVA_OBJECT, "GEOMETRY", 0, 0);
        rs.addRow(factory.createPoint(new Coordinate(x, y)));
        return rs;
    }

    private void testAggregateWithGeometry() throws SQLException {
        deleteDb("spatialIndex");
        Connection conn = getConnection("spatialIndex");
        try {
            Statement st = conn.createStatement();
            st.execute("CREATE AGGREGATE TABLE_ENVELOPE FOR \""+
                    TableEnvelope.class.getName()+"\"");
            st.execute("CREATE TABLE test(the_geom GEOMETRY)");
            st.execute("INSERT INTO test VALUES ('POINT(1 1)'), ('POINT(10 5)')");
            ResultSet rs = st.executeQuery("select TABLE_ENVELOPE(the_geom) from test");
            assertEquals("geometry", rs.getMetaData().
                    getColumnTypeName(1).toLowerCase());
            assertTrue(rs.next());
            assertTrue(rs.getObject(1) instanceof Geometry);
            assertTrue(new Envelope(1, 10, 1, 5).equals(
                    ((Geometry) rs.getObject(1)).getEnvelopeInternal()));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
        deleteDb("spatialIndex");
    }

    /**
     * An aggregate function that calculates the envelope.
     */
    public static class TableEnvelope implements Aggregate {
        private Envelope tableEnvelope;

        @Override
        public int getInternalType(int[] inputTypes) throws SQLException {
            for (int inputType : inputTypes) {
                if (inputType != Value.GEOMETRY) {
                    throw new SQLException("TableEnvelope accept only Geometry argument");
                }
            }
            return Value.GEOMETRY;
        }

        @Override
        public void init(Connection conn) throws SQLException {
            tableEnvelope = null;
        }

        @Override
        public void add(Object value) throws SQLException {
            if (value instanceof Geometry) {
                if (tableEnvelope == null) {
                    tableEnvelope = ((Geometry) value).getEnvelopeInternal();
                } else {
                    tableEnvelope.expandToInclude(((Geometry) value).getEnvelopeInternal());
                }
            }
        }

        @Override
        public Object getResult() throws SQLException {
            return new GeometryFactory().toGeometry(tableEnvelope);
        }
    }

    private void testTableViewSpatialPredicate() throws SQLException {
        deleteDb("spatial");
        Connection conn = getConnection(url);
        try {
            Statement stat = conn.createStatement();
            stat.execute("drop table if exists test");
            stat.execute("drop view if exists test_view");
            stat.execute("create table test(id int primary key, poly geometry)");
            stat.execute("insert into test values(1, 'POLYGON ((1 1, 1 2, 2 2, 1 1))')");
            stat.execute("insert into test values(2, 'POLYGON ((3 1, 3 2, 4 2, 3 1))')");
            stat.execute("insert into test values(3, 'POLYGON ((1 3, 1 4, 2 4, 1 3))')");
            stat.execute("create view test_view as select * from test");

            //Check result with view
            ResultSet rs;
            rs = stat.executeQuery(
                    "select * from test where poly && 'POINT (1.5 1.5)'::Geometry");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertFalse(rs.next());

            rs = stat.executeQuery(
                    "select * from test_view where poly && 'POINT (1.5 1.5)'::Geometry");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertFalse(rs.next());
            rs.close();
        } finally {
            // Close the database
            conn.close();
        }
        deleteDb("spatial");
    }

    /**
     * Check ValueGeometry conversion into SQL script
     */
    private void testValueGeometryScript() throws SQLException {
        ValueGeometry valueGeometry = ValueGeometry.get("POINT(1 1 5)");
        Connection conn = getConnection(url);
        try {
            ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT " + valueGeometry.getSQL());
            assertTrue(rs.next());
            Object obj = rs.getObject(1);
            ValueGeometry g = ValueGeometry.getFromGeometry(obj);
            assertTrue("got: " + g + " exp: " + valueGeometry, valueGeometry.equals(g));
        } finally {
            conn.close();
        }
    }

    /**
     * If the user mutate the geometry of the object, the object cache must not
     * be updated.
     */
    private void testInPlaceUpdate() throws SQLException {
        Connection conn = getConnection(url);
        try {
            ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT 'POINT(1 1)'::geometry");
            assertTrue(rs.next());
            // Mutate the geometry
            ((Geometry) rs.getObject(1)).apply(new AffineTransformation(1, 0,
                    1, 1, 0, 1));
            rs.close();
            rs = conn.createStatement().executeQuery(
                    "SELECT 'POINT(1 1)'::geometry");
            assertTrue(rs.next());
            // Check if the geometry is the one requested
            assertEquals(1, ((Point) rs.getObject(1)).getX());
            assertEquals(1, ((Point) rs.getObject(1)).getY());
            rs.close();
        } finally {
            conn.close();
        }
    }
}
