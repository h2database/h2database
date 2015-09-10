/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import com.vividsolutions.jts.geom.Envelope;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

import org.h2.test.TestBase;
import org.h2.util.Utils;
import org.h2.value.ValueRaster;


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
        testLittleEndian();
        testLittleEndianHexa();
        testExternalRaster();
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
        ValueRaster testRaster = ValueRaster.createEmptyGeoRaster(0, 2, 3, 0.5, 0.5, 0, 0, 0, 10, 20);
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
        String bytesString = "01" +	                                   /* little endian (uint8 ndr) */
                "0000" +                                               /* version (uint16 0) */
                "0300" +                                               /* nBands (uint16 3) */
                "9A9999999999A93F" +                                   /* scaleX (float64 0.050000) */
                "9A9999999999A9BF" +                                   /* scaleY (float64 -0.050000) */
                "000000E02B274A41" +                                   /* ipX (float64 3427927.750000) */
                "0000000077195641" +                                   /* ipY (float64 5793244.000000) */
                "0000000000000000" +                                   /* skewX (float64 0.000000) */
                "0000000000000000" +                                   /* skewY (float64 0.000000) */
                "FFFFFFFF" +                                           /* srid (int32 -1) */
                "0500" +                                               /* width (uint16 5) */
                "0500" +                                               /* height (uint16 5) */
                "44" +                                                 /* 1st band pixel type (8BUI, in memory, hasnodata) */
                "00" +                                                 /* 1st band nodata 0 */
                "FDFEFDFEFEFDFEFEFDF9FAFEFEFCF9FBFDFEFEFDFCFAFEFEFE" + /* 1st band pixels */
                "44" +                                                 /* 2nd band pixel type (8BUI, in memory, hasnodata) */
                "00" +                                                 /* 2nd band nodata 0 */
                "4E627AADD16076B4F9FE6370A9F5FE59637AB0E54F58617087" + /* 2nd band pixels */
                "44" +                                                 /* 3rd band pixel type (8BUI, in memory, hasnodata) */
                "00" +                                                 /* 3rd band nodata 0 */
                "46566487A1506CA2E3FA5A6CAFFBFE4D566DA4CB3E454C5665";  /* 3rd band pixels */

        byte[] bytes = Utils.hexStringToByteArray(bytesString);
        
        InputStream bytesStream = new ByteArrayInputStream(bytes);
        long len = bytes.length;
        ValueRaster testRaster = ValueRaster.createGeoRaster(bytesStream, len, null);
        ValueRaster.RasterMetaData metaData = testRaster.getMetaData();
        assertEquals(3, metaData.numBands);
        assertEquals(0.05, metaData.scaleX);
        assertEquals(-0.05, metaData.scaleY);
        assertEquals(3427927.75, metaData.ipX);
        assertEquals(5793244.00, metaData.ipY);
        assertEquals(0., metaData.skewX);
        assertEquals(0., metaData.skewY);
        assertEquals(5, metaData.width);
        assertEquals(5, metaData.height);
        Envelope env = testRaster.getEnvelope();
        assertEquals(3427927.75, env.getMinX());
        assertEquals(5793243.75, env.getMinY());
        assertEquals(3427928, env.getMaxX());
        assertEquals(5793244, env.getMaxY());
        assertEquals(-1, testRaster.getMetaData().srid);
        // Check bands meta
        assertEquals(3, metaData.bands.length);
        ValueRaster.RasterBandMetaData bandMetaData = metaData.bands[0];
        assertTrue(ValueRaster.PixelType.PT_8BUI == bandMetaData.pixelType);
        assertFalse(bandMetaData.offDB);
        assertTrue(bandMetaData.hasNoData);
        assertEquals(0, bandMetaData.noDataValue);
        bandMetaData = metaData.bands[1];
        assertTrue(ValueRaster.PixelType.PT_8BUI == bandMetaData.pixelType);
        assertFalse(bandMetaData.offDB);
        assertTrue(bandMetaData.hasNoData);
        assertEquals(0, bandMetaData.noDataValue);
        bandMetaData = metaData.bands[2];
        assertTrue(ValueRaster.PixelType.PT_8BUI == bandMetaData.pixelType);
        assertFalse(bandMetaData.offDB);
        assertTrue(bandMetaData.hasNoData);
        assertEquals(0, bandMetaData.noDataValue);
    }

    private void testLittleEndian() throws IOException {
        // Write as little endian
        ValueRaster testRasterLittleEndian = ValueRaster.createEmptyGeoRaster(0, 2, 3, 0.5, 0.5, 0, 0, 0, 10, 20);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        testRasterLittleEndian.getMetaData().writeRasterHeader(byteArrayOutputStream, ByteOrder.LITTLE_ENDIAN);
        // Read the stream
        byte[] dataLittleEndian = byteArrayOutputStream.toByteArray();
        ValueRaster testRaster = ValueRaster.createGeoRaster(new ByteArrayInputStream(dataLittleEndian), dataLittleEndian.length, null);
        ValueRaster.RasterMetaData meta = testRaster.getMetaData();
        assertEquals(0, meta.numBands);
        assertEquals(2, meta.scaleX);
        assertEquals(3, meta.scaleY);
        assertEquals(10, meta.width);
        assertEquals(20, meta.height);
        Envelope env = testRaster.getEnvelope();
        assertEquals(0.5, env.getMinX());
        assertEquals(0.5, env.getMinY());
        assertEquals(20.5, env.getMaxX());
        assertEquals(30.5, env.getMaxY());

    }

    /**
     * Raster unit test using PostGIS WKB.
     * @throws IOException
     */
    private void testLittleEndianHexa() throws IOException {
        String hexwkb = "01" +              /* little endian (uint8 ndr) */
                "0000" +                    /* version (uint16 0) */
                "0100" +                    /* nBands (uint16 1) */
                "0000000000805640" +        /* scaleX (float64 90.0) */
                "00000000008056C0" +        /* scaleY (float64 -90.0) */
                "000000001C992D41" +        /* ipX (float64 969870.0) */
                "00000000E49E2341" +        /* ipY (float64 642930.0) */
                "0000000000000000" +        /* skewX (float64 0) */
                "0000000000000000" +        /* skewY (float64 0) */
                "FFFFFFFF" +                /* SRID (int32 -1) */
                "0300" +                    /* width (uint16 3) */
                "0100" +                    /* height (uint16 1) */
                "45" +                      /* First band type (16BSI, in memory, hasnodata) */
                "0100" +                    /* nodata value (1) */
                "0100" +                    /* pix(0,0) == 1 */
                "B401" +                    /* pix(1,0) == 436 */
                "AF01";                     /* pix(2,0) == 431 */

        byte[] bytes = Utils.hexStringToByteArray(hexwkb);

        InputStream bytesStream = new ByteArrayInputStream(bytes);
        long len = bytes.length;
        ValueRaster testRaster = ValueRaster.createGeoRaster(bytesStream, len, null);
        ValueRaster.RasterMetaData metaData = testRaster.getMetaData();
        assertEquals(1, metaData.numBands);
        assertEquals(90, metaData.scaleX);
        assertEquals(-90, metaData.scaleY);
        assertEquals(969870, metaData.ipX);
        assertEquals(642930, metaData.ipY);
        assertEquals(0, metaData.skewX);
        assertEquals(0, metaData.skewY);
        assertEquals(-1, metaData.srid);
        assertEquals(3, metaData.width);
        assertEquals(1, metaData.height);
        assertTrue(ValueRaster.PixelType.PT_16BSI == metaData.bands[0].pixelType);
        assertFalse(metaData.bands[0].offDB);
        assertTrue(metaData.bands[0].hasNoData);
        assertEquals(1, metaData.bands[0].noDataValue);
    }

    private void testExternalRaster() throws IOException {
        String hexwkb = "00" +              /* big endian (uint8 xdr) */
                "0000" +                    /* version (uint16 0) */
                "0001" +                    /* nBands (uint16 1) */
                "3FF0000000000000" +        /* scaleX (float64 1) */
                "4000000000000000" +        /* scaleY (float64 2) */
                "4008000000000000" +        /* ipX (float64 3) */
                "4010000000000000" +        /* ipY (float64 4) */
                "4014000000000000" +        /* skewX (float64 5) */
                "4018000000000000" +        /* skewY (float64 6) */
                "0000000A" +                /* SRID (int32 10) */
                "0003" +                    /* width (uint16 3) */
                "0002" +                    /* height (uint16 2) */
                "C5" +                      /* First band type (16BSI, on disk, hasnodata) */
                "FFFF" +                    /* nodata value (-1) */
                "03" +                      /* ext band num == 3 */
                "2F746D702F742E74696600";   /* ext band path == /tmp/t.tif */

        byte[] bytes = Utils.hexStringToByteArray(hexwkb);

        InputStream bytesStream = new ByteArrayInputStream(bytes);
        long len = bytes.length;
        ValueRaster testRaster = ValueRaster.createGeoRaster(bytesStream, len, null);
        ValueRaster.RasterMetaData metaData = testRaster.getMetaData();
        assertEquals(0, metaData.version);
        assertEquals(1, metaData.numBands);
        assertEquals(1, metaData.scaleX);
        assertEquals(2, metaData.scaleY);
        assertEquals(3, metaData.ipX);
        assertEquals(4, metaData.ipY);
        assertEquals(5, metaData.skewX);
        assertEquals(6, metaData.skewY);
        assertEquals(10, metaData.srid);
        assertEquals(3, metaData.width);
        assertEquals(2, metaData.height);
        assertTrue(ValueRaster.PixelType.PT_16BSI == metaData.bands[0].pixelType);
        assertTrue(metaData.bands[0].offDB);
        assertTrue(metaData.bands[0].hasNoData);
        assertEquals(-1, metaData.bands[0].noDataValue);
        assertEquals(3, metaData.bands[0].externalBandId);
        assertEquals("/tmp/t.tif\0", metaData.bands[0].externalPath);
    }
}