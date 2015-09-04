/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import com.vividsolutions.jts.geom.Envelope;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;
import org.h2.test.TestBase;
import org.h2.util.Utils;
import org.h2.value.ValueRaster;

import javax.rmi.CORBA.Util;

/**
 * Unit test of Raster type
 * @author Thomas Crevoisier, Jules Party
 */
public class TestGeoRaster extends TestBase {

    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.big = true;
        test.test();
    }
    
    @Override
    public void test() throws Exception {
        if (!config.mvStore && config.mvcc) {
            return;
        }
        if (config.memory && config.mvcc) {
            return;
        }
        testEmptyGeoRaster();
        testGeoRasterWithBands();
        testReadRaster();
        testWriteRasterFromString();
        testSpatialIndex();
    }
 
   
    private void testReadRaster() throws Exception {
        deleteDb("georaster");
        Connection conn;
        conn = getConnection("georaster");
        Statement stat = conn.createStatement();
                stat.execute("create table test(id identity, data raster)");
        
        PreparedStatement prep = conn.prepareStatement(
                "insert into test values(null, ?)");
        byte[] data = new byte[256];
        Random r = new Random(1);
        for (int i = 0; i < 1000; i++) {
            r.nextBytes(data);
            prep.setBinaryStream(1, new ByteArrayInputStream(data), -1);
            prep.execute();
        }

        ResultSet rs = stat.executeQuery("select * from test");
        while (rs.next()) {
            rs.getString(2);
        }
        conn.close();
    }
    
    private void testWriteRasterFromString() throws Exception {
        String bytesString = "01"       // little endian (uint8 ndr)
                + "0000"                // version (uint16 0)
                + "0000"                // nBands (uint16 0)
                + "0000000000000040"    // scaleX (float64 2)
                + "0000000000000840"    // scaleY (float64 3)
                + "000000000000e03F"    // ipX (float64 0.5)
                + "000000000000e03F"    // ipY (float64 0.5)
                + "0000000000000000"    // skewX (float64 0)
                + "0000000000000000"    // skewY (float64 0)
                + "00000000"            // SRID (int32 0)
                + "0a00"                // width (uint16 10)
                + "1400";               // height (uint16 20)
        
        byte[] bytes = Utils.hexStringToByteArray(bytesString);
        
        InputStream bytesStream = new ByteArrayInputStream(bytes);
        
        deleteDb("georaster");
        Connection conn;
        conn = getConnection("georaster");
        Statement stat = conn.createStatement();
                stat.execute("create table test(id identity, data raster)");
        
        PreparedStatement prep = conn.prepareStatement(
                "insert into test values(null, ?)");

        prep.setBinaryStream(1, bytesStream, -1);
        prep.execute();

        ResultSet rs = stat.executeQuery("select * from test");
        rs.next();
        assertTrue(bytesString.equalsIgnoreCase(rs.getString(2)));
        conn.close();
    }
    
    private void testSpatialIndex() throws Exception {
        deleteDb("georaster");

        String bytesString = "01"
                + "0000"
                + "0000"
                + "000000000000F03F"
                + "000000000000F03F"
                + "0000000000000000"
                + "0000000000000000"
                + "0000000000000000"
                + "0000000000000000"
                + "00000000"
                + "0a00"
                + "0a00";
        byte[] bytes = Utils.hexStringToByteArray(bytesString);
        InputStream bytesStream1 = new ByteArrayInputStream(bytes);

        bytesString = "01"
                + "0000"
                + "0000"
                + "000000000000F03F"
                + "000000000000F03F"
                + "0000000000001440"
                + "0000000000001440"
                + "0000000000000000"
                + "0000000000000000"
                + "00000000"
                + "0a00"
                + "0a00";
        bytes = Utils.hexStringToByteArray(bytesString);
        InputStream bytesStream2 = new ByteArrayInputStream(bytes);

        bytesString = "01"
                + "0000"
                + "0000"
                + "000000000000F03F"
                + "000000000000F03F"
                + "0000000000002440"
                + "0000000000001440"
                + "0000000000000000"
                + "0000000000000000"
                + "00000000"
                + "0a00"
                + "0a00";
        bytes = Utils.hexStringToByteArray(bytesString);
        InputStream bytesStream3 = new ByteArrayInputStream(bytes);

        Connection conn;
        conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id identity, data raster)");

        PreparedStatement prep = conn.prepareStatement(
                "insert into test values(null, ?)");
        prep.setBinaryStream(1, bytesStream1, -1);
        prep.execute();
        prep.setBinaryStream(1, bytesStream2, -1);
        prep.execute();
        prep.setBinaryStream(1, bytesStream3, -1);
        prep.execute();

        Statement stat2 = conn.createStatement();
        stat2.execute("create spatial index on test(data)");

        ResultSet rs = stat.executeQuery(
                "select * from test " +
                "where data && 'POINT (1.5 1.5)'::Geometry");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertFalse(rs.next());
        rs.close();
        conn.close();
    }

    public void testEmptyGeoRaster() throws Exception {
        ValueRaster testRaster = ValueRaster.createEmptyGeoRaster(2, 3, 0.5, 0.5, 0, 0, 0, 10, 20);
        Envelope env = testRaster.getEnvelope();
        assertEquals(0.5, env.getMinX());
        assertEquals(0.5, env.getMinY());
        assertEquals(20.5, env.getMaxX());
        assertEquals(30.5, env.getMaxY());
        ValueRaster.RasterMetaData meta = testRaster.getMetaData();
        assertEquals(2, meta.scaleX);
        assertEquals(3, meta.scaleY);
        assertEquals(10, meta.width);
        assertEquals(20, meta.height);
    }

    public void testGeoRasterWithBands() throws Exception {
        String bytesString = "01"
                + "0000"
                + "0300"
                + "9A9999999999A93F"
                + "9A9999999999A9BF"
                + "000000E02B274A41"
                + "0000000077195641"
                + "0000000000000000"
                + "0000000000000000"
                + "E6100000"
                + "0500"
                + "0500"
                + "04"
                + "00"
                + "FDFEFDFEFE"
                + "FDFEFEFDF9"
                + "FAFEFEFCF9"
                + "FBFDFEFEFD"
                + "FCFAFEFEFE"
                + "04"
                + "00"
                + "4E627AADD1"
                + "6076B4F9FE"
                + "6370A9F5FE"
                + "59637AB0E5"
                + "4F58617087"
                + "04"
                + "00"
                + "46566487A1"
                + "506CA2E3FA"
                + "5A6CAFFBFE"
                + "4D566DA4CB"
                + "3E454C5665";
        
        byte[] bytes = Utils.hexStringToByteArray(bytesString);
        
        InputStream bytesStream = new ByteArrayInputStream(bytes);
        long len = bytes.length;
        ValueRaster testRaster = ValueRaster.createGeoRaster(bytesStream, len, null);
        Envelope env = testRaster.getEnvelope();
        assertEquals(env.getMinX(), 3427927.75);
        assertEquals(env.getMinY(), 5793243.75);
        assertEquals(env.getMaxX(), 3427928);
        assertEquals(env.getMaxY(), 5793244);
        assertEquals(testRaster.getMetaData().srid, 4326);
    }
}