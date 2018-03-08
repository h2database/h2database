/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;


import org.h2.api.GeoRaster;
import org.h2.test.TestBase;
import org.h2.util.IOUtils;
import org.h2.util.ImageInputStreamWrapper;
import org.h2.util.RasterUtils;
import org.h2.util.Utils;
import org.h2.util.GeoRasterRenderedImage;
import org.h2.value.Value;
import org.h2.value.ValueBytes;
import org.h2.value.ValueLobDb;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.ImageWriter;
import javax.imageio.stream.FileImageInputStream;
import javax.imageio.stream.ImageInputStream;
import java.awt.*;
import java.awt.Point;
import java.awt.color.ColorSpace;
import java.awt.geom.AffineTransform;
import java.awt.image.BandedSampleModel;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.DataBufferDouble;
import java.awt.image.DataBufferFloat;
import java.awt.image.DataBufferInt;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import java.nio.ByteOrder;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;


/**
 * Unit test of Raster type
 *
 * @author Thomas Crevoisier, CNRS
 * @author Jules Party, CNRS
 * @author Nicolas Fortin, CNRS
 */
public class TestGeoRaster extends TestBase {

    public static String GetUnitTestImage() throws URISyntaxException {
         return new File(TestGeoRaster.class.getResource("h2-logo.png").getFile()).getAbsolutePath();
    }

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
        testWriteRasterFromString();
        testSpatialIndex();
        testLittleEndian();
        testLittleEndianHexa();
        testExternalRaster();
        testStRasterMetaData();
        testRasterCastToGeometry();
        testPngLoading();
        testPngLoadingSQL();
        testImageIOWKBTranslation();
        testWKBTranslationStream();
        testImageFromRaster();
        testMakeEmptyRaster();
        testMakeEmptyRaster2();
        testRasterToString();
        testStBandMetaData();
        testImageIOReadParametersRegion();
        testImageIOReadParametersSubSampling();
        testST_WorldToRasterCoord();
        testST_RasterToWorldCoord();
        testRasterProcessing();
        testRenderedImageCopy();
        testWKBRasterFloat();
        testWKBRasterInt();
        testWKBRasterDouble();
        testST_ImageFromRaster();
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

        PreparedStatement prep =
                conn.prepareStatement("insert into test values(null, ?)");

        prep.setBinaryStream(1, bytesStream, -1);
        prep.execute();

        ResultSet rs = stat.executeQuery("select * from test");
        rs.next();
        assertEquals(bytes, rs.getBytes(2));
        assertTrue(rs.getObject("DATA") instanceof GeoRaster);
        conn.close();
    }

    private void testSpatialIndex() throws Exception {
        deleteDb("georaster");

        String bytesString = "01" + "0000" + "0000" + "000000000000F03F" +
                "000000000000F03F" + "0000000000000000" + "0000000000000000" +
                "0000000000000000" + "0000000000000000" + "00000000" + "0a00" +
                "0a00";
        byte[] bytes = Utils.hexStringToByteArray(bytesString);
        InputStream bytesStream1 = new ByteArrayInputStream(bytes);

        bytesString = "01" + "0000" + "0000" + "000000000000F03F" +
                "000000000000F03F" + "0000000000001440" + "0000000000001440" +
                "0000000000000000" + "0000000000000000" + "00000000" + "0a00" +
                "0a00";
        bytes = Utils.hexStringToByteArray(bytesString);
        InputStream bytesStream2 = new ByteArrayInputStream(bytes);

        bytesString = "01" + "0000" + "0000" + "000000000000F03F" +
                "000000000000F03F" + "0000000000002440" + "0000000000001440" +
                "0000000000000000" + "0000000000000000" + "00000000" + "0a00" +
                "0a00";
        bytes = Utils.hexStringToByteArray(bytesString);
        InputStream bytesStream3 = new ByteArrayInputStream(bytes);

        Connection conn;
        conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id identity, data raster)");

        PreparedStatement prep =
                conn.prepareStatement("insert into test values(null, ?)");
        prep.setBinaryStream(1, bytesStream1, -1);
        prep.execute();
        prep.setBinaryStream(1, bytesStream2, -1);
        prep.execute();
        prep.setBinaryStream(1, bytesStream3, -1);
        prep.execute();

        Statement stat2 = conn.createStatement();
        stat2.execute("create spatial index on test(data)");

        ResultSet rs = stat.executeQuery("select * from test " +
                "where data && 'POINT (1.5 1.5)'::Geometry");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertFalse(rs.next());
        rs.close();
        conn.close();
    }

    public void testEmptyGeoRaster() throws Exception {
        RasterUtils.RasterMetaData meta = new RasterUtils.RasterMetaData
                (RasterUtils.LAST_WKB_VERSION,0, 10,20,0, 2, 3, 0.5, 0.5, 0, 0);
        // POLYGON((0.5 0.5,0.5 60.5,20.5 60.5,20.5 0.5,0.5 0.5))
        Envelope env = meta.getEnvelope();
        assertEquals(0.5, env.getMinX());
        assertEquals(0.5, env.getMinY());
        assertEquals(20.5, env.getMaxX());
        assertEquals(60.5, env.getMaxY());
        assertEquals(2, meta.scaleX);
        assertEquals(3, meta.scaleY);
        assertEquals(10, meta.width);
        assertEquals(20, meta.height);
    }

    public void testGeoRasterWithBands() throws Exception {
        String bytesString =
                "01000003009A9999999999A93F9A9999999999A9BF000000E02B274A41" +
                        "000000007719564100000000000000000000000000000000FF" +
                        "FFFFFF050005004400FDFEFDFEFEFDFEFEFDF9FAFEFEFCF9FB" +
                        "FDFEFEFDFCFAFEFEFE44004E627AADD16076B4F9FE6370A9F5" +
                        "FE59637AB0E54F58617087440046566487A1506CA2E3FA5A6C" +
                        "AFFBFE4D566DA4CB3E454C5665";
        byte[] bytes = Utils.hexStringToByteArray(bytesString);

        InputStream bytesStream = new ByteArrayInputStream(bytes);
        RasterUtils.RasterMetaData metaData = RasterUtils.RasterMetaData
                .fetchMetaData(bytesStream, true);
        assertEquals(3, metaData.numBands);
        assertEquals(0.05, metaData.scaleX);
        assertEquals(-0.05, metaData.scaleY);
        assertEquals(3427927.75, metaData.ipX);
        assertEquals(5793244.00, metaData.ipY);
        assertEquals(0., metaData.skewX);
        assertEquals(0., metaData.skewY);
        assertEquals(5, metaData.width);
        assertEquals(5, metaData.height);
        Envelope env = metaData.getEnvelope();
        assertEquals(3427927.75, env.getMinX());
        assertEquals(5793243.75, env.getMinY());
        assertEquals(3427928, env.getMaxX());
        assertEquals(5793244, env.getMaxY());
        assertEquals(-1, metaData.srid);
        // Check bands meta
        assertEquals(3, metaData.bands.length);
        RasterUtils.RasterBandMetaData bandMetaData = metaData.bands[0];
        assertTrue(RasterUtils.PixelType.PT_8BUI == bandMetaData.pixelType);
        assertFalse(bandMetaData.offDB);
        assertTrue(bandMetaData.hasNoData);
        assertEquals(0, bandMetaData.noDataValue);
        bandMetaData = metaData.bands[1];
        assertTrue(RasterUtils.PixelType.PT_8BUI == bandMetaData.pixelType);
        assertFalse(bandMetaData.offDB);
        assertTrue(bandMetaData.hasNoData);
        assertEquals(0, bandMetaData.noDataValue);
        bandMetaData = metaData.bands[2];
        assertTrue(RasterUtils.PixelType.PT_8BUI == bandMetaData.pixelType);
        assertFalse(bandMetaData.offDB);
        assertTrue(bandMetaData.hasNoData);
        assertEquals(0, bandMetaData.noDataValue);
    }

    private void testLittleEndian() throws IOException {
        // Write as little endian
        RasterUtils.RasterMetaData testRasterLittleEndian = new RasterUtils.RasterMetaData
                (RasterUtils.LAST_WKB_VERSION,0,10,
                        20, 0, 2, 3, 0.5, 0.5, 0, 0 );
        ByteArrayOutputStream byteArrayOutputStream =
                new ByteArrayOutputStream();
        testRasterLittleEndian.writeRasterHeader(byteArrayOutputStream,
                ByteOrder.LITTLE_ENDIAN);
        // Read the stream
        byte[] dataLittleEndian = byteArrayOutputStream.toByteArray();
        RasterUtils.RasterMetaData meta = RasterUtils.RasterMetaData
                .fetchMetaData(new ByteArrayInputStream(dataLittleEndian));
        assertEquals(0, meta.numBands);
        assertEquals(2, meta.scaleX);
        assertEquals(3, meta.scaleY);
        assertEquals(10, meta.width);
        assertEquals(20, meta.height);
        Envelope env = meta.getEnvelope();
        assertEquals(0.5, env.getMinX());
        assertEquals(0.5, env.getMinY());
        assertEquals(20.5, env.getMaxX());
        assertEquals(60.5, env.getMaxY());

    }

    /**
     * Raster unit test using PostGIS WKB.
     *
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
                "45" +                      /* First band type (16BSI, in
                memory, hasnodata) */
                "0100" +                    /* nodata value (1) */
                "0100" +                    /* pix(0,0) == 1 */
                "B401" +                    /* pix(1,0) == 436 */
                "AF01";                     /* pix(2,0) == 431 */

        byte[] bytes = Utils.hexStringToByteArray(hexwkb);

        InputStream bytesStream = new ByteArrayInputStream(bytes);
        RasterUtils.RasterMetaData metaData = RasterUtils.RasterMetaData
                .fetchMetaData(bytesStream, true);
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
        assertTrue(
                RasterUtils.PixelType.PT_16BSI == metaData.bands[0].pixelType);
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
                "C5" +                      /* First band type (16BSI, on
                disk, hasnodata) */
                "FFFF" +                    /* nodata value (-1) */
                "03" +                      /* ext band num == 3 */
                "2F746D702F742E74696600";   /* ext band path == /tmp/t.tif */

        byte[] bytes = Utils.hexStringToByteArray(hexwkb);

        InputStream bytesStream = new ByteArrayInputStream(bytes);
        RasterUtils.RasterMetaData metaData = RasterUtils.RasterMetaData
                .fetchMetaData(bytesStream, true);
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
        assertTrue(
                RasterUtils.PixelType.PT_16BSI == metaData.bands[0].pixelType);
        assertTrue(metaData.bands[0].offDB);
        assertTrue(metaData.bands[0].hasNoData);
        assertEquals(-1, metaData.bands[0].noDataValue);
        assertEquals(3, metaData.bands[0].externalBandId);
        assertEquals("/tmp/t.tif\0", metaData.bands[0].externalPath);
        // Write back metadata into bytes
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        metaData.writeRasterHeader(outputStream, ByteOrder.BIG_ENDIAN);
        // write bands header
        for(int idband = 0;idband < metaData.numBands; idband++) {
            metaData.writeRasterBandHeader(outputStream, idband, ByteOrder.BIG_ENDIAN);
        }
        // Check equality
        assertEquals(bytes, outputStream.toByteArray());
    }

    public void testStRasterMetaData() throws SQLException {
        Connection conn;
        conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(id identity, data raster)");
        // ipx(3) ipY(4) width(7) height(8) scaleX(1) scaleY(2) skewX(5)
        // skewY(6) SRID(10) numbands(0)
        stat.execute("INSERT INTO TEST(data) VALUES " +
                        "" +
                "('00000000003FF000000000000040000000000000004008000000000000401" +
                        "0000000000000401400000000000040180000000000000000000A00070008')");
        ResultSet rs =
                stat.executeQuery("SELECT ST_METADATA(data) meta FROM TEST");
        try {
            assertTrue(rs.next());
            assertEquals(
                    new Object[]{3.0, 4.0, 7, 8, 1.0, 2.0, 5.0, 6.0, 10, 0},
                    (Object[]) rs.getObject(1));
        } finally {
            rs.close();
        }
    }

    /**
     * Check if cast of raster into geometry is going well.
     *
     * @throws SQLException
     */
    public void testRasterCastToGeometry() throws SQLException {
        // ipx(3) ipY(4) width(7) height(8) scaleX(1) scaleY(2) skewX(5)
        // skewY(6) SRID(10) numbands(0)
        final String wkbRaster =
                "00000000003FF000000000000040000000000000004008000000000" +
                        "00040100000000000004014000000000000401800000000" +
                        "00000000000A00070008";
        Connection conn;
        conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(id identity, data raster)");
        stat.execute("INSERT INTO TEST(data) VALUES ('" + wkbRaster + "')");
        ResultSet rs = stat.executeQuery("SELECT data::geometry env FROM TEST");
        try {
            assertTrue(rs.next());
            assertTrue(rs.getObject(1) instanceof Geometry);
            assertEquals("POLYGON ((3 4, 10 46, 50 62, 43 20, 3 4))",
                    rs.getString(1));
        } finally {
            rs.close();
        }
    }

    public void testPngLoading() throws IOException, URISyntaxException {
        // Test loading PNG into Raster
        File testFile = new File(GetUnitTestImage());
        // Fetch ImageRead using ImageIO API then convert it to WKB Raster on
        // the fly
        byte[] data =
                IOUtils.readBytesAndClose(new FileInputStream(testFile), -1);
        Value raster = RasterUtils.getFromImage(ValueBytes.get(data), 0,
                0, 1, 1, 0, 0, 0, null);
        assertTrue(raster != null);
        // Read again bytes to extract metadata
        assertTrue(raster instanceof Value.ValueRasterMarker);
        RasterUtils.RasterMetaData meta = ((Value.ValueRasterMarker)raster)
                .getMetaData();
        assertEquals(4, meta.numBands);
        assertEquals(4, meta.bands.length);
        assertEquals(530, meta.width);
        assertEquals(288, meta.height);
    }

    public void testPngLoadingSQL() throws Exception {
        Connection conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(id identity, data raster)");
        stat.execute("INSERT INTO TEST(data) VALUES (" +
                "ST_RasterFromImage( File_Read('" + GetUnitTestImage() +
                        "'), 47.6443, -2.7766, 1, 1,0, 0, 4326))");
        // Check MetaData
        ResultSet rs = stat.executeQuery("SELECT data rasterdata FROM " +
                "TEST");
        // Read MetaData
        assertTrue(rs.next());
        InputStream is = rs.getBinaryStream(1);
        assertFalse(rs.wasNull());
        RasterUtils.RasterMetaData meta = RasterUtils.RasterMetaData
                .fetchMetaData(is, true);
        assertTrue(meta != null);
        assertEquals(4, meta.numBands);
        assertEquals(4, meta.bands.length);
        assertEquals(530, meta.width);
        assertEquals(288, meta.height);
        assertEquals(47.6443, meta.ipX);
        assertEquals(-2.7766, meta.ipY);
        assertEquals(1, meta.scaleX);
        assertEquals(1, meta.scaleY);
        assertEquals(0, meta.skewX);
        assertEquals(0, meta.skewY);
        assertEquals(4326, meta.srid);
        assertTrue(RasterUtils.PixelType.PT_8BUI == meta.bands[0].pixelType);
        assertTrue(RasterUtils.PixelType.PT_8BUI == meta.bands[1].pixelType);
        assertTrue(RasterUtils.PixelType.PT_8BUI == meta.bands[2].pixelType);
        assertTrue(RasterUtils.PixelType.PT_8BUI == meta.bands[3].pixelType);
        rs.close();
        conn.close();
    }

    public void testImageIOWKBTranslation() throws SQLException, IOException, URISyntaxException {
        Connection conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(id identity, data raster)");
        String imageFile = GetUnitTestImage();
        assertTrue(new File(imageFile).exists());
        PreparedStatement pst = conn.prepareStatement("INSERT INTO TEST(data) VALUES (" +
                "ST_RasterFromImage(File_Read(?), 47" +
                ".6443,  -2.7766, 1, 1,0, 0, 4326))");
        pst.setString(1, imageFile);
        pst.execute();
        // Query WKB Raster binary
        ResultSet rs = stat.executeQuery("SELECT data rasterdata FROM " +
                "TEST");
        assertTrue(rs.next());
        // Convert SQL Blob object into ImageInputStream
        Blob blob = rs.getBlob(1);
        ImageInputStream inputStream = ImageIO.createImageInputStream(blob);
        assertTrue(inputStream != null);
        // Fetch WKB Raster Image reader
        Iterator<ImageReader> readers = ImageIO.getImageReaders(inputStream);
        assertTrue(readers.hasNext());
        ImageReader wkbReader = readers.next();
        // Feed WKB Raster Reader with blob data
        wkbReader.setInput(inputStream);
        // Retrieve data as a BufferedImage
        BufferedImage image = wkbReader.read(wkbReader.getMinIndex());
        // Check Image by comparing all pixels
        FileImageInputStream fis = new FileImageInputStream(new File
                (GetUnitTestImage()));
        ImageReader pngReader = ImageIO.getImageReaders(fis).next();
        pngReader.setInput(fis);
        BufferedImage sourceImage = pngReader.read(0);
        assertEquals(288, image.getHeight());
        assertEquals(530, image.getWidth());
        int pixelsSource[] = sourceImage.getData()
                .getPixels(0, 0, image.getWidth(), image.getHeight(),(int[])
                        null);
        int pixelsDest[] = image.getData().getPixels(0, 0, image.getWidth(),
                image.getHeight(),(int[])null);
        assertEquals(pixelsSource, pixelsDest);
        // Write to disk as BMP file
        Iterator<ImageWriter> writers = ImageIO.getImageWritersBySuffix("png");
        assertTrue(writers.hasNext());
        ImageWriter pngWriter = writers.next();
        File tmpFile = new File(getTestDir
                ("georaster"), "testConv.png");
        if(!tmpFile.getParentFile().exists()) {
            assertTrue(tmpFile.getParentFile().mkdirs());
        }
        RandomAccessFile fileOutputStream = new RandomAccessFile(tmpFile,
                "rw");
        pngWriter.setOutput(ImageIO.createImageOutputStream(fileOutputStream));
        pngWriter.write(new IIOImage(image, null, null));
        fileOutputStream.close();
        rs.close();
        conn.close();
    }

    private static BufferedImage getTestImage(final int width,final int
            height) {
        BufferedImage image = new BufferedImage(width, height, BufferedImage
                .TYPE_INT_RGB);
        WritableRaster raster = image.getRaster();
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int red = x < width / 2 ? 255 : 0;
                int green = y < height / 2 ? 255 : 0;
                int blue = x + y < (width + height) / 2 ? 255 : 0;
                raster.setPixel(x, y, new int[]{red, green, blue});
            }
        }
        return image;
    }

    private static double getTestImagePixelValue(int x, int y, int width) {
        return y * (width * 10) + x;
    }

    private static RenderedImage getFloatTestImage(final int width,final int
            height) {
        DataBufferFloat dataBufferFloat = new DataBufferFloat(width * height
                , 1);
        BandedSampleModel bandedSampleModel = new BandedSampleModel
                (dataBufferFloat.getDataType(), width, height,
                        dataBufferFloat.getNumBanks());
        WritableRaster raster = WritableRaster.createWritableRaster
                (bandedSampleModel, dataBufferFloat, new Point());
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                float pixelValue = (float)getTestImagePixelValue(x, y, width);
                raster.setPixel(x, y, new float[]{pixelValue});
            }
        }
        ColorModel colorModel = new ComponentColorModel(ColorSpace
                .getInstance(ColorSpace.CS_GRAY), false, false, ColorModel
                .OPAQUE, DataBuffer.TYPE_FLOAT);
        return new BufferedImage(colorModel, raster, false, null);
    }

    private static RenderedImage getIntTestImage(final int width,final int
            height) {
        DataBufferInt dataBuffer = new DataBufferInt(width * height
                , 1);
        BandedSampleModel bandedSampleModel = new BandedSampleModel
                (dataBuffer.getDataType(), width, height,
                        dataBuffer.getNumBanks());
        WritableRaster raster = WritableRaster.createWritableRaster
                (bandedSampleModel, dataBuffer, new Point());
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int pixelValue = (int)getTestImagePixelValue(x, y, width);
                raster.setPixel(x, y, new int[]{pixelValue});
            }
        }
        ColorModel colorModel = new ComponentColorModel(ColorSpace
                .getInstance(ColorSpace.CS_GRAY), false, false, ColorModel
                .OPAQUE, DataBuffer.TYPE_INT);
        return new BufferedImage(colorModel, raster, false, null);
    }
    private static RenderedImage getDoubleTestImage(final int width,final int
            height) {
        DataBufferDouble dataBuffer = new DataBufferDouble(width * height
                , 1);
        BandedSampleModel bandedSampleModel = new BandedSampleModel
                (dataBuffer.getDataType(), width, height,
                        dataBuffer.getNumBanks());
        WritableRaster raster = WritableRaster.createWritableRaster
                (bandedSampleModel, dataBuffer, new Point());
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                double pixelValue = getTestImagePixelValue(x, y, width);
                raster.setPixel(x, y, new double[]{pixelValue});
            }
        }
        ColorModel colorModel = new ComponentColorModel(ColorSpace
                .getInstance(ColorSpace.CS_GRAY), false, false, ColorModel
                .OPAQUE, DataBuffer.TYPE_DOUBLE);
        return new BufferedImage(colorModel, raster, false, null);
    }

    public void testWKBTranslationStream() throws SQLException, IOException {
        // Create an image from scratch
        final int width = 50, height = 50;
        BufferedImage image = getTestImage(width, height);
        // Convert into WKB data
        GeoRasterRenderedImage wrapper = GeoRasterRenderedImage.create(image, 1, 1, 0,
                0, 0, 0, 27572, 0.d);
        InputStream wkbStream = wrapper.asWKBRaster();
        Value rasterValue = ValueLobDb.createSmallLob(Value.RASTER, IOUtils
                .readBytesAndClose(wkbStream, -1));
        // Convert WKB Raster into PNG ValueBlob
        Value valueBlobPNG = RasterUtils.asImage(rasterValue, "png", null);
        // Check png
        ImageInputStreamWrapper imageInput = new ImageInputStreamWrapper
                (new ImageInputStreamWrapper.ValueStreamProvider
                        (valueBlobPNG), null);
        ImageReader pngReader = ImageIO.getImageReaders(imageInput).next();
        imageInput.seek(0);
        pngReader.setInput(imageInput);
        BufferedImage sourceImage = pngReader.read(0);
        assertEquals(height, sourceImage.getHeight());
        assertEquals(width, sourceImage.getWidth());
        // Check pixels
        int pixelsSource[] = sourceImage.getData()
                .getPixels(0, 0, sourceImage.getWidth(), sourceImage.getHeight(),(int[])
                        null);
        int pixelsExpected[] = image.getData()
                .getPixels(0, 0, image.getWidth(), image.getHeight(),(int[])
                        null);
        assertEquals(pixelsExpected, pixelsSource);
        // write to temp dir
        File tmpFile = new File(getTestDir
                ("georaster"), "testConvFunction.png");
        if(!tmpFile.getParentFile().exists()) {
            assertTrue(tmpFile.getParentFile().mkdirs());
        }
        FileOutputStream outputStream = new FileOutputStream(tmpFile);
        IOUtils.copyAndClose(valueBlobPNG.getInputStream(), outputStream);
    }



    public void testImageFromRaster() throws SQLException, IOException {
        // Create an image from scratch
        final int width = 50, height = 50;
        BufferedImage expectedImage = getTestImage(width, height);
        // Convert into WKB data
        GeoRasterRenderedImage wrapper = GeoRasterRenderedImage.create(expectedImage, 1, 1, 0,
                0, 0, 0, 27572, 0.);
        InputStream wkbStream = wrapper.asWKBRaster();
        // Transfer to database
        deleteDb("georaster");
        Connection conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(id identity, data raster)");
        PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO TEST(data) VALUES(?)");
        ps.setBlob(1, wkbStream);
        ps.execute();
        ps.close();
        // Query H2, asking for conversion into PNG binary data
        ResultSet rs = stat.executeQuery(
                "SELECT ST_IMAGEFROMRASTER(data, 'png') pngdata FROM test");
        assertTrue(rs.next());
        // Write directly data to a file
        File tmpFile = new File(getTestDir
                ("georaster"), "testImageFromRaster.png");
        if(!tmpFile.getParentFile().exists()) {
            assertTrue(tmpFile.getParentFile().mkdirs());
        }
        FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
        IOUtils.copyAndClose(rs.getBinaryStream("pngdata"), fileOutputStream);
        // Read file
        RandomAccessFile ras = new RandomAccessFile(tmpFile, "rw");
        ImageInputStream inputStream = ImageIO.createImageInputStream(ras);
        ImageReader pngReader =  ImageIO.getImageReaders(inputStream).next();
        pngReader.setInput(inputStream);
        BufferedImage resultImage = pngReader.read(pngReader.getMinIndex());
        // Check pixels
        int pixelsSource[] = resultImage.getData()
                .getPixels(0, 0, resultImage.getWidth(),
                        resultImage.getHeight(), (int[]) null);
        int pixelsExpected[] = expectedImage.getData()
                .getPixels(0, 0, expectedImage.getWidth(),
                        expectedImage.getHeight(), (int[]) null);
        assertEquals(pixelsExpected, pixelsSource);
    }

    public void testMakeEmptyRaster() throws Exception {
        deleteDb("georaster");
        Connection conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(id identity, data raster)");
        // Check make with existing raster
        PreparedStatement st = conn.prepareStatement("INSERT INTO TEST(data) " +
                "values(ST_MAKEEMPTYRASTER(?::raster))");
        st.setBinaryStream(1, GeoRasterRenderedImage.create(getTestImage(10, 10)
                , 1, -1, 0, 0, 0, 0, 27572)
                .asWKBRaster());
        st.execute();
        stat.execute("INSERT INTO TEST(DATA) VALUES(" +
                "ST_MAKEEMPTYRASTER(33,33,0,0,1,-1,0,0,27572))");
        // Check values
        ResultSet rs = stat.executeQuery("SELECT data FROM test order by id");
        assertTrue(rs.next());
        RasterUtils.RasterMetaData metaData = RasterUtils.RasterMetaData
                .fetchMetaData(rs.getBinaryStream(1));
        assertEquals(10, metaData.width);
        assertEquals(10, metaData.height);
        assertEquals(0, metaData.numBands);
        assertEquals(0, metaData.ipX);
        assertEquals(0, metaData.ipY);
        assertEquals(1, metaData.scaleX);
        assertEquals(-1, metaData.scaleY);
        assertEquals(0, metaData.skewX);
        assertEquals(0, metaData.skewY);
        assertEquals(27572, metaData.srid);
        assertTrue(rs.next());
       metaData = RasterUtils.RasterMetaData
                .fetchMetaData(rs.getBinaryStream(1));
        assertEquals(33, metaData.width);
        assertEquals(33, metaData.height);
        assertEquals(0, metaData.numBands);
        assertEquals(0, metaData.ipX);
        assertEquals(0, metaData.ipY);
        assertEquals(1, metaData.scaleX);
        assertEquals(-1, metaData.scaleY);
        assertEquals(0, metaData.skewX);
        assertEquals(0, metaData.skewY);
        assertEquals(27572, metaData.srid);
        conn.close();
    }


    public void testMakeEmptyRaster2() throws Exception {
        deleteDb("georaster");
        Connection conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(id identity, data raster)");
        stat.execute("INSERT INTO TEST(DATA) VALUES(" +
                "ST_MAKEEMPTYRASTER(33,33,0,0,1))");
        // Check values
        ResultSet rs = stat.executeQuery("SELECT data FROM test order by id");
        assertTrue(rs.next());
        RasterUtils.RasterMetaData metaData = RasterUtils.RasterMetaData
                .fetchMetaData(rs.getBinaryStream(1));
        assertEquals(33, metaData.width);
        assertEquals(33, metaData.height);
        assertEquals(0, metaData.numBands);
        assertEquals(0, metaData.ipX);
        assertEquals(0, metaData.ipY);
        assertEquals(1, metaData.scaleX);
        assertEquals(-1, metaData.scaleY);
        assertEquals(0, metaData.skewX);
        assertEquals(0, metaData.skewY);
        assertEquals(0, metaData.srid);
        conn.close();
    }

    public void testRasterToString() throws Exception {
        deleteDb("georaster");
        Connection conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(id identity, data raster)");
        // Check make with existing raster
        PreparedStatement st = conn.prepareStatement("INSERT INTO TEST(data) " +
                "values(?)");
        st.setBinaryStream(1, GeoRasterRenderedImage
                .create(getTestImage(10, 10), 1, -1, 0, 0, 0, 0,
                        27572).asWKBRaster());
        st.execute();
        ResultSet rs = stat.executeQuery("SELECT data FROM test order by id");
        assertTrue(rs.next());
        assertEquals("w:10 h:10 bands:3 srid:27572 x:0.00000 y:0.00000 " +
                "scalex:1.00000 scaley:-1.00000 skewx:0.00000 skewy:0.00000",
                rs.getString(1));
        conn.close();
    }


    public void testStBandMetaData() throws SQLException, IOException {
        deleteDb("georaster");
        Connection conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(id identity, data raster)");
        // Check make with existing raster
        PreparedStatement st = conn.prepareStatement("INSERT INTO TEST(data) " +
                "values(?)");
        st.setBinaryStream(1, GeoRasterRenderedImage
                .create(getTestImage(10, 10), 1, -1, 0, 0, 0, 0,
                        27572).asWKBRaster());
        st.execute();
        ResultSet rs =
                stat.executeQuery("SELECT ST_BANDMETADATA(data, 1) meta FROM " +
                        "TEST");
        try {
            assertTrue(rs.next());
            assertEquals(
                    new Object[]{"8BUI", null, false, ""},
                    (Object[]) rs.getObject(1));
        } finally {
            rs.close();
        }
        rs =
                stat.executeQuery("SELECT ST_BANDMETADATA(data, 2) meta FROM " +
                        "TEST");
        try {
            assertTrue(rs.next());
            assertEquals(
                    new Object[]{"8BUI", null, false, ""},
                    (Object[]) rs.getObject(1));
        } finally {
            rs.close();
        }
        rs =
                stat.executeQuery("SELECT ST_BANDMETADATA(data, 3) meta FROM " +
                        "TEST");
        try {
            assertTrue(rs.next());
            assertEquals(
                    new Object[]{"8BUI", null, false, ""},
                    (Object[]) rs.getObject(1));
        } finally {
            rs.close();
        }
    }



    public void testImageIOReadParametersRegion() throws SQLException, IOException, URISyntaxException {
        deleteDb("georaster");
        Connection conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(id identity, data raster)");
        stat.execute("INSERT INTO TEST(data) VALUES (" +
                "ST_RasterFromImage(File_Read('" + GetUnitTestImage() + "'), 47" +
                ".6443,  -2.7766, 1, 1,0, 0, 4326))");

        // Query WKB Raster binary
        ResultSet rs = stat.executeQuery("SELECT data rasterdata FROM " +
                "TEST");
        assertTrue(rs.next());
        // Convert SQL Blob object into ImageInputStream
        Blob blob = rs.getBlob(1);
        ImageInputStream inputStream = ImageIO.createImageInputStream(blob);
        assertTrue(inputStream != null);
        // Fetch WKB Raster Image reader
        Iterator<ImageReader> readers = ImageIO.getImageReaders(inputStream);
        assertTrue(readers.hasNext());
        ImageReader wkbReader = readers.next();
        // Feed WKB Raster Reader with blob data
        wkbReader.setInput(inputStream);
        // Retrieve data as a BufferedImage
        ImageReadParam param = wkbReader.getDefaultReadParam();
        Rectangle rect = new Rectangle(5, 3, 10, 15);
        param.setSourceRegion(rect);
        BufferedImage image = wkbReader.read(wkbReader.getMinIndex(), param);
        assertEquals(10, image.getWidth());
        assertEquals(15, image.getHeight());
        // Read source
        ImageInputStream is = ImageIO.createImageInputStream(new RandomAccessFile(GetUnitTestImage(), "r"));
        ImageReader ir = ImageIO.getImageReaders(is).next();
        ImageReadParam srcParam = ir.getDefaultReadParam();
        srcParam.setSourceRegion(rect);
        ir.setInput(is);
        BufferedImage srcImage = ir.read(ir.getMinIndex(), srcParam);
        int[] pixelsSource = srcImage.getRGB(0, 0, srcImage.getWidth(), srcImage.getHeight(), null, 0, image.getWidth());
        int[] pixels = image.getRGB(0, 0, image.getWidth(), image.getHeight(), null, 0, image.getWidth());
        assertEquals(pixelsSource, pixels);
    }


    /**
     * Check that provided images are (visually) equals. Pixel are drawn on
     * the same destination graphics then 8 bpp colors are read.
     * @param expectedImage Expected result
     * @param imageB Obtained image
     */
    public void assertImageEquals(RenderedImage expectedImage, RenderedImage imageB) {
        BufferedImage expectedImageDest = new BufferedImage(expectedImage.getWidth(),expectedImage.getHeight(), BufferedImage.TYPE_INT_ARGB);
        BufferedImage destB = new BufferedImage(imageB.getWidth(),imageB.getHeight(), BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = expectedImageDest.createGraphics();
        g.drawRenderedImage(expectedImage, new AffineTransform());
        g.dispose();
        g = destB.createGraphics();
        g.drawRenderedImage(imageB, new AffineTransform());
        g.dispose();
        int[] pixelsExpected = expectedImageDest.getData().getPixels(0,0,expectedImageDest.getWidth(),
                expectedImageDest.getHeight(),(int[])null);
        int[] pixelsSource = destB.getData().getPixels(0, 0, destB.getWidth(), destB.getHeight(), (int[]) null);
        assertEquals(pixelsExpected, pixelsSource);
    }

    public void testImageIOReadParametersSubSampling() throws SQLException, IOException, URISyntaxException {
        deleteDb("georaster");
        Connection conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(id identity, data raster)");
        stat.execute("INSERT INTO TEST(data) VALUES (" +
                "ST_RasterFromImage(File_Read('" + GetUnitTestImage() + "'), 47" +
                ".6443,  -2.7766, 1, 1,0, 0, 4326))");

        // Query WKB Raster binary
        ResultSet rs = stat.executeQuery("SELECT data rasterdata FROM " +
                "TEST");
        assertTrue(rs.next());
        // Convert SQL Blob object into ImageInputStream
        Blob blob = rs.getBlob(1);
        ImageInputStream inputStream = ImageIO.createImageInputStream(blob);
        assertTrue(inputStream != null);
        // Fetch WKB Raster Image reader
        Iterator<ImageReader> readers = ImageIO.getImageReaders(inputStream);
        assertTrue(readers.hasNext());
        ImageReader wkbReader = readers.next();
        // Feed WKB Raster Reader with blob data
        wkbReader.setInput(inputStream);
        // Retrieve data as a BufferedImage
        ImageReadParam param = wkbReader.getDefaultReadParam();
        int subX = 2;
        int subY = 3;
        int offsetX = 0;
        int offsetY = 0;
        Rectangle rect = new Rectangle(5, 5, 225, 288);
        // Read source with standard driver
        ImageInputStream is = ImageIO.createImageInputStream(new RandomAccessFile(GetUnitTestImage(), "r"));
        ImageReader ir = ImageIO.getImageReaders(is).next();
        ImageReadParam srcParam = ir.getDefaultReadParam();
        srcParam.setSourceSubsampling(subX, subY, offsetX, offsetY);
        srcParam.setSourceRegion(rect);
        ir.setInput(is);
        BufferedImage srcImage = ir.read(ir.getMinIndex(), srcParam);
        // Same with WKB Driver
        param.setSourceSubsampling(subX, subY, offsetX, offsetY);
        param.setSourceRegion(rect);
        RenderedImage image = wkbReader.readAsRenderedImage(wkbReader.getMinIndex(),
                param);
        // WKBRaster images claims that there is tiling support. (using rows)
        assertEquals(image.getHeight(), image.getNumYTiles());
        assertEquals(srcImage.getWidth(), image.getWidth());
        assertEquals(srcImage.getHeight(), image.getHeight());
        assertImageEquals(srcImage, image);
    }

    private static BufferedImage getImageRegion(Blob blob, Rectangle
            rectangle) throws IOException {
        ImageInputStream inputStream = ImageIO.createImageInputStream(blob);
        // Fetch WKB Raster Image reader
        Iterator<ImageReader> readers = ImageIO.getImageReaders(inputStream);
        ImageReader wkbReader = readers.next();
        // Feed WKB Raster Reader with blob data
        wkbReader.setInput(inputStream);
        // Retrieve data as a BufferedImage
        ImageReadParam param = wkbReader.getDefaultReadParam();
        param.setSourceRegion(rectangle);
        return wkbReader.read(wkbReader.getMinIndex(), param);
    }


    public void testST_WorldToRasterCoord() throws Exception {
        deleteDb("georaster");
        Connection conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT ST_WorldToRasterCoord(" +
                "st_makeemptyraster(10, 10, 1.44754, -2.100, 0.001)," +
                " 1.457, -2.101);");
        assertTrue(rs.next());
        assertEquals(new Object[]{10, 2}, (Object[])rs.getObject(1));
        rs = stat.executeQuery("select st_worldtorastercoord( " +
                "st_makeemptyraster(100, 100, 555, 256, 2.5), 570, 200);");
        assertTrue(rs.next());
        assertEquals(new Object[]{7, 23}, (Object[])rs.getObject(1));
        rs = stat.executeQuery("select st_worldtorastercoord( " +
                "st_makeemptyraster(256, 256, 6000000 , 300000, 2.5), " +
                "6000200, 350000);");
        assertTrue(rs.next());
        assertEquals(new Object[]{81, -19999}, (Object[])rs.getObject(1));
        conn.close();
    }


    public void testST_RasterToWorldCoord() throws Exception {
        deleteDb("georaster");
        Connection conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT ST_RasterToWorldCoord(" +
                "st_makeemptyraster(10, 10, 1.44754, -2.100, 0.001)," +
                " 10, 2);");
        assertTrue(rs.next());
        org.locationtech.jts.geom.Point pos =
                (org.locationtech.jts.geom.Point)rs.getObject(1);
        assertEquals(1.45653999999999995, pos.getX());
        assertEquals(-2.10099999999999998, pos.getY());
        rs = stat.executeQuery("SELECT ST_RasterToWorldCoord( " +
                "st_makeemptyraster(100, 100, 555, 256, 2.5), 7, 23);");
        assertTrue(rs.next());
        pos =
                (org.locationtech.jts.geom.Point)rs.getObject(1);
        assertEquals(570., pos.getX());
        assertEquals(201., pos.getY());
        rs = stat.executeQuery("SELECT ST_RasterToWorldCoord( " +
                "st_makeemptyraster(256, 256, 6000000 , 300000, 2.5), 81, " +
                "-19999);");
        assertTrue(rs.next());
        pos =
                (org.locationtech.jts.geom.Point)rs.getObject(1);
        assertEquals(6000200., pos.getX());
        assertEquals(350000., pos.getY());
        conn.close();
    }

    /**
     * Function test with RenderedImage as return value.
     * @param source Raster
     * @param factor Factor of rescaling 0.5 for half-size
     * @return resized raster
     * @throws IOException
     */
    public static GeoRaster rescale(GeoRaster source, double factor)
            throws IOException {
        RasterUtils.RasterMetaData meta = source.getMetaData();
        BufferedImage resizedImage = new BufferedImage((int)(source.getWidth() * factor),
                (int)(source.getHeight() * factor), BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = resizedImage.createGraphics();
        g.drawRenderedImage(source, AffineTransform.getScaleInstance(factor,
                factor));
        g.dispose();
        return GeoRasterRenderedImage.create(resizedImage, meta.scaleX / factor, meta
                        .scaleY / factor, meta.ipX, meta.ipY, meta.skewX / factor,
                meta.skewY / factor, meta.srid).setMaxRowCache(1);
    }

    /**
     * Check if a function having GeoRaster api as parameter and return value
     * is working.
     * @throws Exception
     */
    public void testRasterProcessing() throws Exception {
        deleteDb("georaster");
        Connection conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        // Declare custom function for rescaling image
        stat.execute("DROP ALIAS IF EXISTS ST_RESCALE");
        stat.execute("CREATE ALIAS ST_RESCALE FOR \"" +
                TestGeoRaster.class.getName() + ".rescale\"");
        stat.execute("drop table if exists test");
        stat.execute("create table test(id identity, data raster)");
        // Create table with test image
        PreparedStatement st = conn.prepareStatement("INSERT INTO TEST(data) " +
                "values(?)");
        st.setBinaryStream(1, GeoRasterRenderedImage.create(getTestImage(10, 10)
                , 1, -1, 0, 0, 0, 0, 27572)
                .asWKBRaster());
        st.execute();
        // Rescale the image twice
        ResultSet rs = stat.executeQuery("SELECT ST_RESCALE(ST_RESCALE(DATA, " +
                "2), 0.5) rast from test");
        assertEquals("RASTER", rs.getMetaData().getColumnTypeName(1));
        assertTrue(rs.next());
        RasterUtils.RasterMetaData metaData = RasterUtils.RasterMetaData
                .fetchMetaData(rs.getBinaryStream(1));
        assertEquals(10, metaData.width);
        assertEquals(10, metaData.height);
        // Get image portion
        BufferedImage image = getImageRegion(rs.getBlob(1), new Rectangle(3,3,3,
                3));
        assertEquals(new int[]{-1, -1, -16711681, -1, -1, -16711681, -65281,
                -65281, -16777216}, image.getRGB(0,0,3,3,null,0,3));
        rs.close();
        // Store the resized image in the table
        stat.execute("DROP TABLE IF EXISTS RESIZED");
        stat.execute("CREATE TABLE RESIZED(rast RASTER) AS SELECT ST_RESCALE" +
                "(ST_RESCALE(DATA, 2), 0.5) rast from test");
        rs = stat.executeQuery("SELECT * from resized");
        assertTrue(rs.next());
        metaData = RasterUtils.RasterMetaData
                .fetchMetaData(rs.getBinaryStream(1));
        assertEquals(10, metaData.width);
        assertEquals(10, metaData.height);
        rs.close();
        // Store the resized image in the table without casting
        stat.execute("DROP TABLE IF EXISTS RESIZED");
        stat.execute("CREATE VIEW RESIZED AS SELECT ST_RESCALE" +
                "(ST_RESCALE(DATA, 2), 0.5) rast from test");
        rs = stat.executeQuery("SELECT * from resized");
        assertTrue(rs.next());
        assertEquals("RASTER", rs.getMetaData().getColumnTypeName(1));
        assertTrue(rs.getObject(1) instanceof GeoRaster);
        rs.close();
        conn.close();
    }


    /**
     * {@link RenderedImage#getData(Rectangle)} should return a copy of the
     * content)
     * @throws SQLException
     * @throws IOException
     */
    public void testRenderedImageCopy() throws SQLException, IOException {
        deleteDb("georaster");
        Connection conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        // Create an image from scratch
        final int width = 50, height = 50;
        BufferedImage image = getTestImage(width, height);
        stat.execute("drop table if exists testcopy");
        stat.execute("create table testcopy(id identity, the_raster raster)");
        PreparedStatement st = conn.prepareStatement("INSERT INTO testcopy" +
                "(the_raster) values(?)");
        st.setBinaryStream(1, GeoRasterRenderedImage.create(image
                , 1, -1, 0, 0, 0, 0, 27572)
                .asWKBRaster());
        st.execute();
        // Query
        ResultSet rs = stat.executeQuery("SELECT the_raster from testcopy");
        assertTrue(rs.next());
        assertTrue(rs.getObject(1) instanceof RenderedImage);
        RenderedImage wkbRasterImage = (RenderedImage)rs.getObject(1);
        Raster rasterCopy = wkbRasterImage.getData();
        assertTrue(rasterCopy.getDataBuffer() instanceof DataBufferByte);
    }

    /**
     * Test WKBRaster created from banded float type image
     * @throws SQLException
     * @throws IOException
     */
    public void testWKBRasterFloat() throws SQLException, IOException {
        deleteDb("georaster");
        Connection conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        // Create an image from scratch
        final int width = 50, height = 50;
        RenderedImage image = getFloatTestImage(width, height);
        stat.execute("drop table if exists testcopy");
        stat.execute("create table testcopy(id identity, the_raster raster)");
        PreparedStatement st = conn.prepareStatement("INSERT INTO testcopy" +
                "(the_raster) values(?)");
        st.setBinaryStream(1, GeoRasterRenderedImage.create(image
                , 1, -1, 0, 0, 0, 0, 27572)
                .asWKBRaster());
        st.execute();
        // Query, and check if value are not altered
        ResultSet rs = stat.executeQuery("SELECT the_raster from testcopy");
        assertTrue(rs.next());
        RasterUtils.RasterMetaData metaData = RasterUtils.RasterMetaData
                .fetchMetaData(rs.getBinaryStream(1), true);
        assertTrue(RasterUtils.PixelType.PT_32BF == metaData.bands[0]
                .pixelType);
        assertTrue(rs.getObject(1) instanceof RenderedImage);
        RenderedImage wkbRasterImage = (RenderedImage)rs.getObject(1);
        Raster rasterCopy = wkbRasterImage.getData();
        assertTrue(rasterCopy.getDataBuffer() instanceof DataBufferFloat);
        for(int y=0; y < wkbRasterImage.getHeight(); y++) {
            for(int x=0; x < wkbRasterImage.getWidth(); x++) {
                float[] val = rasterCopy.getPixel(x, y, (float[])null);
                assertEquals(1, val.length);
                assertEquals(getTestImagePixelValue(x, y, wkbRasterImage
                        .getWidth()), val[0]);
            }
        }
    }



    /**
     * Test WKBRaster created from banded int type image
     * @throws SQLException
     * @throws IOException
     */
    public void testWKBRasterInt() throws SQLException, IOException {
        deleteDb("georaster");
        Connection conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        // Create an image from scratch
        final int width = 50, height = 50;
        RenderedImage image = getIntTestImage(width, height);
        stat.execute("drop table if exists testcopy");
        stat.execute("create table testcopy(id identity, the_raster raster)");
        PreparedStatement st = conn.prepareStatement("INSERT INTO testcopy" +
                "(the_raster) values(?)");
        st.setBinaryStream(1, GeoRasterRenderedImage.create(image
                , 1, -1, 0, 0, 0, 0, 27572)
                .asWKBRaster());
        st.execute();
        // Query, and check if value are not altered
        ResultSet rs = stat.executeQuery("SELECT the_raster from testcopy");
        assertTrue(rs.next());
        RasterUtils.RasterMetaData metaData = RasterUtils.RasterMetaData
                .fetchMetaData(rs.getBinaryStream(1), true);
        assertTrue(RasterUtils.PixelType.PT_32BSI == metaData.bands[0]
                .pixelType);
        assertTrue(rs.getObject(1) instanceof RenderedImage);
        RenderedImage wkbRasterImage = (RenderedImage)rs.getObject(1);
        Raster rasterCopy = wkbRasterImage.getData();
        assertTrue(rasterCopy.getDataBuffer() instanceof DataBufferInt);
        for(int y=0; y < wkbRasterImage.getHeight(); y++) {
            for(int x=0; x < wkbRasterImage.getWidth(); x++) {
                float[] val = rasterCopy.getPixel(x, y, (float[])null);
                assertEquals(1, val.length);
                assertEquals(getTestImagePixelValue(x, y, wkbRasterImage
                        .getWidth()), val[0]);
            }
        }
    }



    /**
     * Test WKBRaster created from banded double type image
     * @throws SQLException
     * @throws IOException
     */
    public void testWKBRasterDouble() throws SQLException, IOException {
        deleteDb("georaster");
        Connection conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        // Create an image from scratch
        final int width = 50, height = 50;
        RenderedImage image = getDoubleTestImage(width, height);
        stat.execute("drop table if exists testcopy");
        stat.execute("create table testcopy(id identity, the_raster raster)");
        PreparedStatement st = conn.prepareStatement("INSERT INTO testcopy" +
                "(the_raster) values(?)");
        st.setBinaryStream(1, GeoRasterRenderedImage.create(image
                , 1, -1, 0, 0, 0, 0, 27572)
                .asWKBRaster());
        st.execute();
        // Query, and check if value are not altered
        ResultSet rs = stat.executeQuery("SELECT the_raster from testcopy");
        assertTrue(rs.next());
        RasterUtils.RasterMetaData metaData = RasterUtils.RasterMetaData
                .fetchMetaData(rs.getBinaryStream(1), true);
        assertTrue(RasterUtils.PixelType.PT_64BF == metaData.bands[0]
                .pixelType);
        assertTrue(rs.getObject(1) instanceof RenderedImage);
        RenderedImage wkbRasterImage = (RenderedImage)rs.getObject(1);
        Raster rasterCopy = wkbRasterImage.getData();
        assertTrue(rasterCopy.getDataBuffer() instanceof DataBufferDouble);
        for(int y=0; y < wkbRasterImage.getHeight(); y++) {
            for(int x=0; x < wkbRasterImage.getWidth(); x++) {
                float[] val = rasterCopy.getPixel(x, y, (float[])null);
                assertEquals(1, val.length);
                assertEquals(getTestImagePixelValue(x, y, wkbRasterImage
                        .getWidth()), val[0]);
            }
        }
    }

    private void testST_ImageFromRaster() throws SQLException, IOException {
        deleteDb("georaster");
        Connection conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        // Create an image from scratch
        final int width = 50, height = 50;
        RenderedImage image = getTestImage(width, height);
        stat.execute("drop table if exists testcopy");
        stat.execute("create table testcopy(id identity, the_raster raster)");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO testcopy" +
                "(the_raster) values(?)");
        ps.setBinaryStream(1, GeoRasterRenderedImage.create(image
                , 1, -1, 0, 0, 0, 0, 27572)
                .asWKBRaster());
        ps.execute();
        String outputFile = "target/test.png";
        stat.execute("SELECT FILE_WRITE(ST_IMAGEFROMRASTER(the_raster, 'png'), '" + outputFile + "') FROM testcopy");
        ResultSet rsImage = stat.executeQuery("SELECT ST_RasterFromImage( File_Read('" + outputFile + "'), 0, 0, 1, -1,0, 0, 27572)");
        assertTrue(rsImage.next());
        assertTrue(rsImage.getObject(1) instanceof RenderedImage);
        RenderedImage wkbRasterImage = (RenderedImage)rsImage.getObject(1);
        Raster rasterOutput = wkbRasterImage.getData();
        Raster rasterInput = image.getData();
        assertEquals(rasterInput.getNumBands(),rasterOutput.getNumBands());
        for (int y = 0; y < wkbRasterImage.getHeight(); y++) {
            for (int x = 0; x < wkbRasterImage.getWidth(); x++) {
                int[] valInput = rasterInput.getPixel(x, y, (int[]) null);
                int[] valOutput = rasterOutput.getPixel(x, y, (int[]) null);
                assertEquals(valInput, valOutput);
            }
        }        
    }
}