/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import static org.h2.util.Bits.INT_VH_BE;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutorService;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.h2.api.ErrorCode;
import org.h2.compress.CompressDeflate;
import org.h2.compress.CompressLZF;
import org.h2.compress.CompressNo;
import org.h2.compress.Compressor;
import org.h2.compress.LZFInputStream;
import org.h2.compress.LZFOutputStream;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * A tool to losslessly compress data, and expand the compressed data again.
 */
public class CompressTool {

    public static final String KANZI_OUTPUT_CLASS_NAME = "io.github.flanglet.kanzi.io.CompressedOutputStream";
    public static final String KANZI_INPUT_CLASS_NAME = "io.github.flanglet.kanzi.io.CompressedInputStream";
    public static final String BZIP2_OUTPUT_CLASS_NAME //
            = "org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream";
    public static final String BZIP2_INPUT_CLASS_NAME //
            = "org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream";

    private static final int MAX_BUFFER_SIZE = 3 * Constants.IO_BUFFER_SIZE_COMPRESS;

    private byte[] buffer;

    private CompressTool() {
        // don't allow construction
    }

    /**
     * Creates a BZip2 compressing output stream using reflection.
     */
    public static OutputStream createBZip2OutputStream(OutputStream baseOutputStream) {
        try {
            // Try Apache Commons Compress first
            Class<?> clazz = Class.forName(BZIP2_OUTPUT_CLASS_NAME);
            Constructor<?> constructor = clazz.getConstructor(OutputStream.class);
            return (OutputStream) constructor.newInstance(baseOutputStream);
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException
                | InvocationTargetException e) {
            throw new RuntimeException("BZip2 compression requires Apache Commons Compress library. "
                    + "Add commons-compress to your classpath.", e);
        }
    }

    /**
     * Creates a BZip2 decompressing input stream using reflection.
     */
    public static InputStream createBZip2InputStream(InputStream inputStream) {
        try {
            // Try Apache Commons Compress first
            Class<?> clazz = Class.forName(BZIP2_INPUT_CLASS_NAME);
            Constructor<?> constructor = clazz.getConstructor(InputStream.class);
            return (InputStream) constructor.newInstance(inputStream);
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException
                | InvocationTargetException e) {
            throw new RuntimeException("BZip2 compression requires Apache Commons Compress library. "
                    + "Add commons-compress to your classpath.", e);
        }
    }

    /**
     * Creates a Kanzi compressing output stream using reflection.
     */
    public static OutputStream createKanziOutputStream(OutputStream baseOutputStream, ExecutorService executor) {
        try {
            // Load Kanzi classes using reflection
            Class<?> clazz = Class.forName(KANZI_OUTPUT_CLASS_NAME);

            // Create configuration map with proper Kanzi parameters
            java.util.Map<String, Object> configMap = new java.util.HashMap<>();

            // Best compression settings (brute tested on a 1.7 GB database,
            // 4.7GB SQL file)
            // 88658331 kanzi -x64 -b 256m -t RLT+PACK+LZP -e TPAQX
            // 88654035 kanzi -x64 -b 256m -t RLT+PACK+LZP+RLT -e TPAQX
            // 85411430 kanzi -x64 -b 256m -t TEXT+RLT+LZP+PACK -e TPAQX
            // 85397152 kanzi -x64 -b 256m -t TEXT+RLT+LZP+PACK+RLT -e TPAQX

            configMap.put("transform", "TEXT+RLT+LZP+PACK+RLT");// Good for SQL
                                                                // dump
            configMap.put("entropy", "TPAQX"); // Text and structured data
            configMap.put("blockSize", 32 * 1024 * 1024); // 32MB blocks
            configMap.put("checksum", 64); // Enable checksums

            configMap.put("pool", executor); // Multi-threaded
            if (Runtime.getRuntime().freeMemory() < 8L * 1024 * 1024 * 1024) {
                configMap.put("jobs", 4);
            } else {
                configMap.put("jobs", Runtime.getRuntime().availableProcessors() / 2);
            }

            Constructor<?> constructor = clazz.getConstructor(OutputStream.class, java.util.Map.class);
            return (OutputStream) constructor.newInstance(baseOutputStream, configMap);

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    "Kanzi compression requires Kanzi library. "
                            + "Add kanzi.jar to your classpath. Download from: https://github.com/flanglet/kanzi-java",
                    e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Kanzi compression: " + e.getMessage(), e);
        }
    }

    /**
     * Creates a Kanzi decompressing input stream using reflection.
     */
    public static InputStream createKanziInputStream(InputStream inputStream, ExecutorService executor) {
        try {
            // Load Kanzi classes using reflection
            Class<?> clazz = Class.forName(KANZI_INPUT_CLASS_NAME);

            // Create configuration map with proper Kanzi parameters
            java.util.Map<String, Object> configMap = new java.util.HashMap<>();

            // Basic compression settings
            configMap.put("pool", executor); // Multi-threaded
            configMap.put("jobs", Runtime.getRuntime().availableProcessors());

            Constructor<?> constructor = clazz.getConstructor(InputStream.class, java.util.Map.class);
            // workaround Zero byte EOF issue
            // it has been fixed only recently so we should still guard for a
            // while
            return new ZeroBytesEOFInputStream((InputStream) constructor.newInstance(inputStream, configMap));

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    "Kanzi compression requires Kanzi library. "
                            + "Add kanzi.jar to your classpath. Download from: https://github.com/flanglet/kanzi-java",
                    e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Kanzi compression: " + e.getMessage(), e);
        }
    }

    private byte[] getBuffer(int min) {
        if (min > MAX_BUFFER_SIZE) {
            return Utils.newBytes(min);
        }
        if (buffer == null || buffer.length < min) {
            buffer = Utils.newBytes(min);
        }
        return buffer;
    }

    /**
     * Get a new instance. Each instance uses a separate buffer, so multiple
     * instances can be used concurrently. However each instance alone is not
     * multithreading safe.
     *
     * @return a new instance
     */
    public static CompressTool getInstance() {
        return new CompressTool();
    }

    /**
     * Compressed the data using the specified algorithm. If no algorithm is
     * supplied, LZF is used
     *
     * @param in
     *            the byte array with the original data
     * @param algorithm
     *            the algorithm (LZF, DEFLATE)
     * @return the compressed data
     */
    public byte[] compress(byte[] in, String algorithm) {
        int len = in.length;
        if (in.length < 5) {
            algorithm = "NO";
        }
        Compressor compress = getCompressor(algorithm);
        byte[] buff = getBuffer((len < 100 ? len + 100 : len) * 2);
        int newLen = compress(in, in.length, compress, buff);
        return Utils.copyBytes(buff, newLen);
    }

    private static int compress(byte[] in, int len, Compressor compress, byte[] out) {
        out[0] = (byte) compress.getAlgorithm();
        int start = 1 + writeVariableInt(out, 1, len);
        int newLen = compress.compress(in, 0, len, out, start);
        if (newLen > len + start || newLen <= 0) {
            out[0] = Compressor.NO;
            System.arraycopy(in, 0, out, start, len);
            newLen = len + start;
        }
        return newLen;
    }

    /**
     * Expands the compressed data.
     *
     * @param in
     *            the byte array with the compressed data
     * @return the uncompressed data
     */
    public byte[] expand(byte[] in) {
        if (in.length == 0) {
            throw DbException.get(ErrorCode.COMPRESSION_ERROR);
        }
        int algorithm = in[0];
        Compressor compress = getCompressor(algorithm);
        try {
            int len = readVariableInt(in, 1);
            int start = 1 + getVariableIntLength(len);
            byte[] buff = Utils.newBytes(len);
            compress.expand(in, start, in.length - start, buff, 0, len);
            return buff;
        } catch (Exception e) {
            throw DbException.get(ErrorCode.COMPRESSION_ERROR, e);
        }
    }

    /**
     * INTERNAL
     *
     * @param in
     *            compressed data
     * @param out
     *            uncompressed result
     * @param outPos
     *            the offset at the output array
     */
    public static void expand(byte[] in, byte[] out, int outPos) {
        int algorithm = in[0];
        Compressor compress = getCompressor(algorithm);
        try {
            int len = readVariableInt(in, 1);
            int start = 1 + getVariableIntLength(len);
            compress.expand(in, start, in.length - start, out, outPos, len);
        } catch (Exception e) {
            throw DbException.get(ErrorCode.COMPRESSION_ERROR, e);
        }
    }

    /**
     * Read a variable size integer using Rice coding.
     *
     * @param buff
     *            the buffer
     * @param pos
     *            the position
     * @return the integer
     */
    public static int readVariableInt(byte[] buff, int pos) {
        int x = buff[pos++] & 0xff;
        if (x < 0x80) {
            return x;
        }
        if (x < 0xc0) {
            return ((x & 0x3f) << 8) + (buff[pos] & 0xff);
        }
        if (x < 0xe0) {
            return ((x & 0x1f) << 16) + ((buff[pos++] & 0xff) << 8) + (buff[pos] & 0xff);
        }
        if (x < 0xf0) {
            return ((x & 0xf) << 24) + ((buff[pos++] & 0xff) << 16) + ((buff[pos++] & 0xff) << 8) + (buff[pos] & 0xff);
        }
        return (int) INT_VH_BE.get(buff, pos);
    }

    /**
     * Write a variable size integer using Rice coding. Negative values need 5
     * bytes.
     *
     * @param buff
     *            the buffer
     * @param pos
     *            the position
     * @param x
     *            the value
     * @return the number of bytes written (0-5)
     */
    public static int writeVariableInt(byte[] buff, int pos, int x) {
        if (x < 0) {
            buff[pos++] = (byte) 0xf0;
            INT_VH_BE.set(buff, pos, x);
            return 5;
        } else if (x < 0x80) {
            buff[pos] = (byte) x;
            return 1;
        } else if (x < 0x4000) {
            buff[pos++] = (byte) (0x80 | (x >> 8));
            buff[pos] = (byte) x;
            return 2;
        } else if (x < 0x20_0000) {
            buff[pos++] = (byte) (0xc0 | (x >> 16));
            buff[pos++] = (byte) (x >> 8);
            buff[pos] = (byte) x;
            return 3;
        } else if (x < 0x1000_0000) {
            INT_VH_BE.set(buff, pos, x | 0xe000_0000);
            return 4;
        } else {
            buff[pos++] = (byte) 0xf0;
            INT_VH_BE.set(buff, pos, x);
            return 5;
        }
    }

    /**
     * Get a variable size integer length using Rice coding. Negative values
     * need 5 bytes.
     *
     * @param x
     *            the value
     * @return the number of bytes needed (0-5)
     */
    public static int getVariableIntLength(int x) {
        if (x < 0) {
            return 5;
        } else if (x < 0x80) {
            return 1;
        } else if (x < 0x4000) {
            return 2;
        } else if (x < 0x20_0000) {
            return 3;
        } else if (x < 0x1000_0000) {
            return 4;
        } else {
            return 5;
        }
    }

    private static Compressor getCompressor(String algorithm) {
        if (algorithm == null) {
            algorithm = "LZF";
        }
        int idx = algorithm.indexOf(' ');
        String options = null;
        if (idx > 0) {
            options = algorithm.substring(idx + 1);
            algorithm = algorithm.substring(0, idx);
        }
        int a = getCompressAlgorithm(algorithm);
        Compressor compress = getCompressor(a);
        compress.setOptions(options);
        return compress;
    }

    /**
     * INTERNAL
     *
     * @param algorithm
     *            to translate into index
     * @return index of the specified algorithm
     */
    private static int getCompressAlgorithm(String algorithm) {
        algorithm = StringUtils.toUpperEnglish(algorithm);
        if ("NO".equals(algorithm)) {
            return Compressor.NO;
        } else if ("LZF".equals(algorithm)) {
            return Compressor.LZF;
        } else if ("DEFLATE".equals(algorithm)) {
            return Compressor.DEFLATE;
        } else {
            throw DbException.get(ErrorCode.UNSUPPORTED_COMPRESSION_ALGORITHM_1, algorithm);
        }
    }

    private static Compressor getCompressor(int algorithm) {
        switch (algorithm) {
        case Compressor.NO:
            return new CompressNo();
        case Compressor.LZF:
            return new CompressLZF();
        case Compressor.DEFLATE:
            return new CompressDeflate();
        default:
            throw DbException.get(ErrorCode.UNSUPPORTED_COMPRESSION_ALGORITHM_1, Integer.toString(algorithm));
        }
    }

    /**
     * INTERNAL
     *
     * @param out
     *            stream
     * @param compressionAlgorithm
     *            to be used
     * @param entryName
     *            in a zip file
     * @return compressed stream
     */
    public static OutputStream wrapOutputStream(OutputStream out, String compressionAlgorithm, String entryName) {
        return wrapOutputStream(out, compressionAlgorithm, entryName, null);
    }

    /**
     * INTERNAL
     *
     * @param out
     *            stream
     * @param compressionAlgorithm
     *            to be used
     * @param entryName
     *            in a zip file
     * @param executor
     *            for supervising the parallel execution (KANZI only)
     * @return compressed stream
     */
    public static OutputStream wrapOutputStream(OutputStream out, String compressionAlgorithm, String entryName,
            ExecutorService executor) {
        try {
            CompressionType compressionType = CompressionType.from(compressionAlgorithm);
            switch (compressionType) {
            case GZIP:
                return new GZIPOutputStream(out);
            case ZIP:
                ZipOutputStream z = new ZipOutputStream(out);
                z.putNextEntry(new ZipEntry(entryName));
                return z;
            case BZIP2:
                return createBZip2OutputStream(out);
            case KANZI:
                return createKanziOutputStream(out, executor);
            case DEFLATE:
                return new DeflaterOutputStream(out);
            case LZF:
                return new LZFOutputStream(out);
            default:
                return out;
            }
        } catch (Exception e) {
            throw DbException.get(ErrorCode.UNSUPPORTED_COMPRESSION_ALGORITHM_1, compressionAlgorithm);
        }
    }

    /**
     * INTERNAL
     *
     * @param in
     *            stream
     * @param compressionAlgorithm
     *            to be used
     * @param entryName
     *            in a zip file
     * @return in stream or null if there is no such entry
     */
    public static InputStream wrapInputStream(InputStream in, String compressionAlgorithm, String entryName) {
        return wrapInputStream(in, compressionAlgorithm, entryName, null);
    }

    /**
     * INTERNAL
     *
     * @param in
     *            stream
     * @param compressionAlgorithm
     *            to be used
     * @param entryName
     *            in a zip file
     * @param executor
     *            for supervising the parallel execution (KANZI only)
     * @return in stream or null if there is no such entry
     */
    public static InputStream wrapInputStream(InputStream in, String compressionAlgorithm, String entryName,
            ExecutorService executor) {
        try {
            CompressionType compressionType = CompressionType.from(compressionAlgorithm);
            switch (compressionType) {
            case GZIP:
                return new GZIPInputStream(in);
            case ZIP:
                ZipInputStream z = new ZipInputStream(in);
                while (true) {
                    ZipEntry entry = z.getNextEntry();
                    if (entry == null) {
                        return null;
                    }
                    if (entryName.equals(entry.getName())) {
                        break;
                    }
                }
                return z;
            case BZIP2:
                return createBZip2InputStream(in);
            case KANZI:
                return createKanziInputStream(in, executor);
            case DEFLATE:
                return in = new InflaterInputStream(in);
            case LZF:
                return new LZFInputStream(in);
            default:
                return in;
            }
        } catch (Exception e) {
            throw DbException.get(ErrorCode.UNSUPPORTED_COMPRESSION_ALGORITHM_1, compressionAlgorithm);
        }
    }

}
