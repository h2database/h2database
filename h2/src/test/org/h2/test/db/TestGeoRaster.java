/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.h2.test.TestBase;
import org.h2.util.IOUtils;
import org.h2.util.ImageInputStreamWrapper;
import org.h2.util.RasterUtils;
import org.h2.util.Utils;
import org.h2.util.WKBRasterWrapper;
import org.h2.value.Value;
import org.h2.value.ValueBytes;
import org.h2.value.ValueLobDb;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.ImageWriter;
import javax.imageio.stream.FileImageInputStream;
import javax.imageio.stream.ImageInputStream;
import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;


/**
 * Unit test of Raster type
 *
 * @author Thomas Crevoisier
 * @author Jules Party
 * @author Nicolas Fortin
 */
public class TestGeoRaster extends TestBase {
    private static String UNIT_TEST_IMAGE = new File(TestGeoRaster.class
    .getResource("h2-logo.png").getFile()).getAbsolutePath();

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
        testRasterToString();
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
                (RasterUtils.LAST_WKB_VERSION,0, 2, 3, 0.5, 0.5, 0, 0, 0, 10,
                        20);
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
                (RasterUtils.LAST_WKB_VERSION,0, 2, 3, 0.5, 0.5, 0, 0, 0, 10,
                        20);
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

    public void testPngLoading() throws IOException {
        // Test loading PNG into Raster
        File testFile = new File(UNIT_TEST_IMAGE);
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
                "ST_RasterFromImage( File_Read('" + UNIT_TEST_IMAGE +
                        "'), 47.6443, -2.7766, 1, 1,0, 0, 4326))");
        // Check MetaData
        ResultSet rs = stat.executeQuery("SELECT data rasterdata FROM " +
                "TEST");
        // Read MetaData
        assertTrue(rs.next());
        RasterUtils.RasterMetaData meta = RasterUtils.RasterMetaData
                .fetchMetaData(rs.getBinaryStream(1), true);
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
        assertTrue(RasterUtils.PixelType.PT_8BSI == meta.bands[0].pixelType);
        assertTrue(RasterUtils.PixelType.PT_8BSI == meta.bands[1].pixelType);
        assertTrue(RasterUtils.PixelType.PT_8BSI == meta.bands[2].pixelType);
        assertTrue(RasterUtils.PixelType.PT_8BSI == meta.bands[3].pixelType);
        rs.close();
        conn.close();
    }

    public void testImageIOWKBTranslation() throws SQLException, IOException {
        Connection conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(id identity, data raster)");
        stat.execute("INSERT INTO TEST(data) VALUES (" +
                "ST_RasterFromImage(File_Read('"+UNIT_TEST_IMAGE+"'), 47" +
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
        BufferedImage image = wkbReader.read(wkbReader.getMinIndex());
        // Check Image by comparing all pixels
        FileImageInputStream fis = new FileImageInputStream(new File
                (UNIT_TEST_IMAGE));
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

    public void testWKBTranslationStream() throws SQLException, IOException {
        // Create an image from scratch
        final int width = 50, height = 50;
        BufferedImage image = getTestImage(width, height);
        WritableRaster raster = image.getRaster();
        // Convert into WKB data
        WKBRasterWrapper wrapper = WKBRasterWrapper.create(raster, 1, 1, 0,
                0, 0, 0, 27572, 0);
        InputStream wkbStream = wrapper.toWKBRasterStream();
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
        WritableRaster raster = expectedImage.getRaster();
        // Convert into WKB data
        WKBRasterWrapper wrapper = WKBRasterWrapper.create(raster, 1, 1, 0,
                0, 0, 0, 27572, 0);
        InputStream wkbStream = wrapper.toWKBRasterStream();
        // Transfer to database
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
        Connection conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(id identity, data raster)");
        // Check make with existing raster
        PreparedStatement st = conn.prepareStatement("INSERT INTO TEST(data) " +
                "values(ST_MAKEEMPTYRASTER(?::raster))");
        st.setBinaryStream(1, WKBRasterWrapper.create(getTestImage(10,10)
                .getRaster(), 1, -1, 0, 0, 0, 0,27572,0)
                .toWKBRasterStream());
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


    public void testRasterToString() throws Exception {
        Connection conn = getConnection("georaster");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(id identity, data raster)");
        // Check make with existing raster
        PreparedStatement st = conn.prepareStatement("INSERT INTO TEST(data) " +
                "values(?)");
        st.setBinaryStream(1, WKBRasterWrapper
                .create(getTestImage(10, 10).getRaster(), 1, -1, 0, 0, 0, 0,
                        27572, 0).toWKBRasterStream());
        st.execute();
        ResultSet rs = stat.executeQuery("SELECT data FROM test order by id");
        assertTrue(rs.next());
        assertEquals("w:10 h:10 bands:3 srid:27572 x:0.00000 y:0.00000 " +
                "scalex:1.00000 scaley:-1.00000 skewx:0.00000 skewy:0.00000",
                rs.getString(1));
        conn.close();
    }
}