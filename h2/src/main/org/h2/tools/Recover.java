/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.zip.CRC32;
import org.h2.compress.CompressLZF;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.DbObject;
import org.h2.engine.MetaRecord;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SimpleRow;
import org.h2.security.SHA256;
import org.h2.store.Data;
import org.h2.store.DataHandler;
import org.h2.store.DataReader;
import org.h2.store.FileLister;
import org.h2.store.FileStore;
import org.h2.store.FileStoreInputStream;
import org.h2.store.LobStorage;
import org.h2.store.Page;
import org.h2.store.PageFreeList;
import org.h2.store.PageLog;
import org.h2.store.PageStore;
import org.h2.util.IOUtils;
import org.h2.util.IntArray;
import org.h2.util.MathUtils;
import org.h2.util.New;
import org.h2.util.SmallLRUCache;
import org.h2.util.StatementBuilder;
import org.h2.util.TempFileDeleter;
import org.h2.util.Tool;
import org.h2.util.Utils;
import org.h2.value.Value;
import org.h2.value.ValueLob;
import org.h2.value.ValueLobDb;
import org.h2.value.ValueLong;

/**
 * Helps recovering a corrupted database.
 * @h2.resource
 */
public class Recover extends Tool implements DataHandler {

    private String databaseName;
    private int block;
    private int storageId;
    private String storageName;
    private int recordLength;
    private int valueId;
    private boolean trace;
    private ArrayList<MetaRecord> schema;
    private HashSet<Integer> objectIdSet;
    private HashMap<Integer, String> tableMap;
    private HashMap<String, String> columnTypeMap;
    private boolean remove;

    private int pageSize;
    private FileStore store;
    private int[] parents;

    private Stats stat;

    /**
     * Statistic data
     */
    class Stats {

        /**
         * The empty space in bytes in a data leaf pages.
         */
        long pageDataEmpty;

        /**
         * The number of bytes used for data.
         */
        int pageDataRows;

        /**
         * The number of bytes used for the page headers.
         */
        int pageDataHead;

        /**
         * The count per page type.
         */
        int[] pageTypeCount = new int[Page.TYPE_STREAM_DATA + 2];

        /**
         * The number of free pages.
         */
        int free;
    }

    /**
     * Options are case sensitive. Supported options are:
     * <table>
     * <tr><td>[-help] or [-?]</td>
     * <td>Print the list of options</td></tr>
     * <tr><td>[-dir &lt;dir&gt;]</td>
     * <td>The directory (default: .)</td></tr>
     * <tr><td>[-db &lt;database&gt;]</td>
     * <td>The database name (all databases if not set)</td></tr>
     * <tr><td>[-trace]</td>
     * <td>Print additional trace information</td></tr>
     * </table>
     * Encrypted databases need to be decrypted first.
     * @h2.resource
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws SQLException {
        new Recover().runTool(args);
    }

    /**
     * Dumps the contents of a database file to a human readable text file. This
     * text file can be used to recover most of the data. This tool does not
     * open the database and can be used even if the database files are
     * corrupted. A database can get corrupted if there is a bug in the database
     * engine or file system software, or if an application writes into the
     * database file that doesn't understand the the file format, or if there is
     * a hardware problem.
     *
     * @param args the command line arguments
     */
    public void runTool(String... args) throws SQLException {
        String dir = ".";
        String db = null;
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if ("-dir".equals(arg)) {
                dir = args[++i];
            } else if ("-db".equals(arg)) {
                db = args[++i];
            } else if ("-removePassword".equals(arg)) {
                remove = true;
            } else if ("-trace".equals(arg)) {
                trace = true;
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else {
                throwUnsupportedOption(arg);
            }
        }
        process(dir, db);
    }

    /**
     * INTERNAL
     */
    public static Reader readClob(String fileName) throws IOException {
        return new BufferedReader(new InputStreamReader(readBlob(fileName), "UTF-8"));
    }

    /**
     * INTERNAL
     */
    public static InputStream readBlob(String fileName) throws IOException {
        return new BufferedInputStream(IOUtils.openFileInputStream(fileName));
    }

    /**
     * INTERNAL
     */
    public static Value.ValueBlob readBlobDb(Connection conn, long lobId, long precision) {
        DataHandler h = ((JdbcConnection) conn).getSession().getDataHandler();
        LobStorage lobStorage = h.getLobStorage();
        return ValueLobDb.create(Value.BLOB, lobStorage, null, LobStorage.TABLE_TEMP, lobId, precision);
    }

    /**
     * INTERNAL
     */
    public static Value.ValueClob readClobDb(Connection conn, long lobId, long precision) {
        DataHandler h = ((JdbcConnection) conn).getSession().getDataHandler();
        LobStorage lobStorage = h.getLobStorage();
        return ValueLobDb.create(Value.CLOB, lobStorage, null, LobStorage.TABLE_TEMP, lobId, precision);
    }

    private void trace(String message) {
        if (trace) {
            out.println(message);
        }
    }

    private void traceError(String message, Throwable t) {
        out.println(message + ": " + t.toString());
        if (trace) {
            t.printStackTrace(out);
        }
    }

    /**
     * Dumps the contents of a database to a SQL script file.
     *
     * @param dir the directory
     * @param db the database name (null for all databases)
     */
    public static void execute(String dir, String db) throws SQLException {
        try {
            new Recover().process(dir, db);
        } catch (DbException e) {
            throw DbException.toSQLException(e);
        }
    }

    private void process(String dir, String db) {
        ArrayList<String> list = FileLister.getDatabaseFiles(dir, db, true);
        if (list.size() == 0) {
            printNoDatabaseFilesFound(dir, db);
        }
        for (String fileName : list) {
            if (fileName.endsWith(Constants.SUFFIX_PAGE_FILE)) {
                dumpPageStore(fileName);
            } else if (fileName.endsWith(Constants.SUFFIX_LOB_FILE)) {
                dumpLob(fileName, true);
                dumpLob(fileName, false);
            }
        }
    }

    private PrintWriter getWriter(String fileName, String suffix) {
        fileName = fileName.substring(0, fileName.length() - 3);
        String outputFile = fileName + suffix;
        trace("Created file: " + outputFile);
        return new PrintWriter(IOUtils.getWriter(IOUtils.openFileOutputStream(outputFile, false)));
    }

    private void writeDataError(PrintWriter writer, String error, byte[] data) {
        writer.println("-- ERROR: " + error + " block: " + block + " storageId: "
                + storageId + " recordLength: " + recordLength + " valueId: " + valueId);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < data.length; i++) {
            int x = data[i] & 0xff;
            if (x >= ' ' && x < 128) {
                sb.append((char) x);
            } else {
                sb.append('?');
            }
        }
        writer.println("-- dump: " + sb.toString());
        sb = new StringBuilder();
        for (int i = 0; i < data.length; i++) {
            int x = data[i] & 0xff;
            sb.append(' ');
            if (x < 16) {
                sb.append('0');
            }
            sb.append(Integer.toHexString(x));
        }
        writer.println("-- dump: " + sb.toString());
    }

    private void dumpLob(String fileName, boolean lobCompression) {
        OutputStream fileOut = null;
        FileStore fileStore = null;
        int size = 0;
        String n = fileName + (lobCompression ? ".comp" : "") + ".txt";
        InputStream in = null;
        try {
            fileOut = IOUtils.openFileOutputStream(n, false);
            fileStore = FileStore.open(null, fileName, "r");
            fileStore.init();
            in = new BufferedInputStream(new FileStoreInputStream(fileStore, this, lobCompression, false));
            byte[] buffer = new byte[Constants.IO_BUFFER_SIZE];
            while (true) {
                int l = in.read(buffer);
                if (l < 0) {
                    break;
                }
                fileOut.write(buffer, 0, l);
                size += l;
            }
            fileOut.close();
        } catch (Throwable e) {
            // this is usually not a problem, because we try both compressed and
            // uncompressed
        } finally {
            IOUtils.closeSilently(fileOut);
            IOUtils.closeSilently(in);
            closeSilently(fileStore);
        }
        if (size == 0) {
            try {
                IOUtils.delete(n);
            } catch (Exception e) {
                traceError(n, e);
            }
        }
    }

    private String getSQL(String column, Value v) {
        if (v instanceof ValueLob) {
            ValueLob lob = (ValueLob) v;
            byte[] small = lob.getSmall();
            if (small == null) {
                String file = lob.getFileName();
                if (lob.getType() == Value.BLOB) {
                    return "READ_BLOB('" + file + ".txt')";
                }
                return "READ_CLOB('" + file + ".txt')";
            }
        } else if (v instanceof ValueLobDb) {
            ValueLobDb lob = (ValueLobDb) v;
            byte[] small = lob.getSmall();
            if (small == null) {
                int type = lob.getType();
                long id = lob.getLobId();
                long precision = lob.getPrecision();
                String m;
                String columnType;
                if (type == Value.BLOB) {
                    columnType = "BLOB";
                    m = "READ_BLOB_DB";
                } else {
                    columnType = "CLOB";
                    m = "READ_CLOB_DB";
                }
                columnTypeMap.put(column, columnType);
                return m + "(" + id + ", " + precision + ")";
            }
        }
        return v.getSQL();
    }

    private void setDatabaseName(String name) {
        databaseName = name;
    }

    private void dumpPageStore(String fileName) {
        setDatabaseName(fileName.substring(0, fileName.length() - Constants.SUFFIX_PAGE_FILE.length()));
        PrintWriter writer = null;
        stat = new Stats();
        try {
            writer = getWriter(fileName, ".sql");
            writer.println("CREATE ALIAS IF NOT EXISTS READ_BLOB FOR \"" + this.getClass().getName() + ".readBlob\";");
            writer.println("CREATE ALIAS IF NOT EXISTS READ_CLOB FOR \"" + this.getClass().getName() + ".readClob\";");
            writer.println("CREATE ALIAS IF NOT EXISTS READ_BLOB_DB FOR \"" + this.getClass().getName() + ".readBlobDb\";");
            writer.println("CREATE ALIAS IF NOT EXISTS READ_CLOB_DB FOR \"" + this.getClass().getName() + ".readClobDb\";");
            resetSchema();
            store = FileStore.open(null, fileName, remove ? "rw" : "r");
            long length = store.length();
            try {
                store.init();
            } catch (Exception e) {
                writeError(writer, e);
            }
            Data s = Data.create(this, 128);
            store.seek(0);
            store.readFully(s.getBytes(), 0, 128);
            s.setPos(48);
            pageSize = s.readInt();
            int writeVersion = s.readByte();
            int readVersion = s.readByte();
            writer.println("-- pageSize: " + pageSize +
                    " writeVersion: " + writeVersion +
                    " readVersion: " + readVersion);
            if (pageSize < PageStore.PAGE_SIZE_MIN || pageSize > PageStore.PAGE_SIZE_MAX) {
                pageSize = SysProperties.PAGE_SIZE;
                writer.println("-- ERROR: page size; using " + pageSize);
            }
            int pageCount = (int) (length / pageSize);
            parents = new int[pageCount];
            s = Data.create(this, pageSize);
            for (int i = 3; i < pageCount; i++) {
                s.reset();
                store.seek(i * pageSize);
                store.readFully(s.getBytes(), 0, 32);
                s.readByte();
                s.readShortInt();
                parents[i] = s.readInt();
            }
            int logKey = 0, logFirstTrunkPage = 0, logFirstDataPage = 0;
            s = Data.create(this, pageSize);
            for (int i = 1;; i++) {
                if (i == 3) {
                    break;
                }
                s.reset();
                store.seek(i * pageSize);
                store.readFully(s.getBytes(), 0, pageSize);
                CRC32 crc = new CRC32();
                crc.update(s.getBytes(), 4, pageSize - 4);
                int expected = (int) crc.getValue();
                int got = s.readInt();
                long writeCounter = s.readLong();
                int key = s.readInt();
                int firstTrunkPage = s.readInt();
                int firstDataPage = s.readInt();
                if (expected == got) {
                    logKey = key;
                    logFirstTrunkPage = firstTrunkPage;
                    logFirstDataPage = firstDataPage;
                }
                writer.println("-- head " + i +
                        ": writeCounter: " + writeCounter +
                        " log key: " + key + " trunk: " + firstTrunkPage + "/" + firstDataPage +
                        " crc expected " + expected +
                        " got " + got + " (" + (expected == got ? "ok" : "different") + ")");
            }
            writer.println("-- firstTrunkPage: " + logFirstTrunkPage +
                    " firstDataPage: " + logFirstDataPage);

            PrintWriter devNull = new PrintWriter(new OutputStream() {
                public void write(int b) {
                    // ignore
                }
            });
            dumpPageStore(devNull, pageCount);
            stat = new Stats();
            schema.clear();
            objectIdSet = New.hashSet();
            dumpPageStore(writer, pageCount);
            writeSchema(writer);
            try {
                dumpPageLogStream(writer, logKey, logFirstTrunkPage, logFirstDataPage);
            } catch (EOFException e) {
                // ignore
            }
            writer.println("---- Statistics ----------");
            writer.println("-- page count: " + pageCount + " free: " + stat.free);
            writer.println("-- page data head: " + stat.pageDataHead + " empty: " + stat.pageDataEmpty + " rows: " + stat.pageDataRows);
            for (int i = 0; i < stat.pageTypeCount.length; i++) {
                int count = stat.pageTypeCount[i];
                if (count > 0) {
                    writer.println("-- page count type: " + i + " " + (100 * count / pageCount) + "% count: " + count);
                }
            }

            writer.close();
        } catch (Throwable e) {
            writeError(writer, e);
        } finally {
            IOUtils.closeSilently(writer);
            closeSilently(store);
        }
    }

    private void dumpPageStore(PrintWriter writer, int pageCount) {
        Data s = Data.create(this, pageSize);
        for (int page = 3; page < pageCount; page++) {
            s = Data.create(this, pageSize);
            store.seek(page * pageSize);
            store.readFully(s.getBytes(), 0, pageSize);
            int type = s.readByte();
            switch (type) {
            case Page.TYPE_EMPTY:
                stat.pageTypeCount[type]++;
                continue;
            }
            boolean last = (type & Page.FLAG_LAST) != 0;
            type &= ~Page.FLAG_LAST;
            if (!PageStore.checksumTest(s.getBytes(), page, pageSize)) {
                writer.println("-- ERROR: page " + page + " checksum mismatch type: " + type);
            }
            s.readShortInt();
            switch (type) {
            // type 1
            case Page.TYPE_DATA_LEAF: {
                stat.pageTypeCount[type]++;
                int parentPageId = s.readInt();
                setStorage(s.readVarInt());
                int columnCount = s.readVarInt();
                int entries = s.readShortInt();
                writer.println("-- page " + page + ": data leaf " + (last ? "(last)" : "") + " parent: " + parentPageId +
                        " table: " + storageId + " entries: " + entries + " columns: " + columnCount);
                dumpPageDataLeaf(writer, s, last, page, columnCount, entries);
                break;
            }
            // type 2
            case Page.TYPE_DATA_NODE: {
                stat.pageTypeCount[type]++;
                int parentPageId = s.readInt();
                setStorage(s.readVarInt());
                int rowCount = s.readInt();
                int entries = s.readShortInt();
                writer.println("-- page " + page + ": data node " + (last ? "(last)" : "") + " parent: " + parentPageId +
                        " entries: " + entries + " rowCount: " + rowCount);
                dumpPageDataNode(writer, s, page, entries);
                break;
            }
            // type 3
            case Page.TYPE_DATA_OVERFLOW:
                stat.pageTypeCount[type]++;
                writer.println("-- page " + page + ": data overflow " + (last ? "(last)" : ""));
                break;
            // type 4
            case Page.TYPE_BTREE_LEAF: {
                stat.pageTypeCount[type]++;
                int parentPageId = s.readInt();
                setStorage(s.readVarInt());
                int entries = s.readShortInt();
                writer.println("-- page " + page + ": b-tree leaf " + (last ? "(last)" : "") + " parent: " + parentPageId +
                        " index: " + storageId + " entries: " + entries);
                if (trace) {
                    dumpPageBtreeLeaf(writer, s, entries, !last);
                }
                break;
            }
            // type 5
            case Page.TYPE_BTREE_NODE:
                stat.pageTypeCount[type]++;
                int parentPageId = s.readInt();
                setStorage(s.readVarInt());
                writer.println("-- page " + page + ": b-tree node" + (last ? "(last)" : "") +  " parent: " + parentPageId +
                        " index: " + storageId);
                dumpPageBtreeNode(writer, s, page, !last);
                break;
            // type 6
            case Page.TYPE_FREE_LIST:
                stat.pageTypeCount[type]++;
                writer.println("-- page " + page + ": free list " + (last ? "(last)" : ""));
                stat.free += dumpPageFreeList(writer, s, page, pageCount);
                break;
            // type 7
            case Page.TYPE_STREAM_TRUNK:
                stat.pageTypeCount[type]++;
                writer.println("-- page " + page + ": log trunk");
                break;
            // type 8
            case Page.TYPE_STREAM_DATA:
                stat.pageTypeCount[type]++;
                writer.println("-- page " + page + ": log data");
                break;
            default:
                writer.println("-- ERROR page " + page + " unknown type " + type);
                break;
            }
        }
    }

    private void dumpPageLogStream(PrintWriter writer, int logKey, int logFirstTrunkPage, int logFirstDataPage) throws IOException {
        Data s = Data.create(this, pageSize);
        DataReader in = new DataReader(
                new PageInputStream(writer, this, store, logKey, logFirstTrunkPage, logFirstDataPage, pageSize)
        );
        writer.println("---- Transaction log ----------");
        CompressLZF compress = new CompressLZF();
        while (true) {
            int x = in.read();
            if (x < 0) {
                break;
            }
            if (x == PageLog.NOOP) {
                // ignore
            } else if (x == PageLog.UNDO) {
                int pageId = in.readVarInt();
                int size = in.readVarInt();
                byte[] data = new byte[pageSize];
                if (size == 0) {
                    in.readFully(data, 0, pageSize);
                } else if (size == 1) {
                    // empty
                } else {
                    byte[] compressBuffer = new byte[size];
                    in.readFully(compressBuffer, 0, size);
                    compress.expand(compressBuffer, 0, size, data, 0, pageSize);
                }
                String typeName = "";
                int type = data[0];
                boolean last = (type & Page.FLAG_LAST) != 0;
                type &= ~Page.FLAG_LAST;
                switch (type) {
                case Page.TYPE_EMPTY:
                    typeName = "empty";
                    break;
                case Page.TYPE_DATA_LEAF:
                    typeName = "data leaf " + (last ? "(last)" : "");
                    break;
                case Page.TYPE_DATA_NODE:
                    typeName = "data node " + (last ? "(last)" : "");
                    break;
                case Page.TYPE_DATA_OVERFLOW:
                    typeName = "data overflow " + (last ? "(last)" : "");
                    break;
                case Page.TYPE_BTREE_LEAF:
                    typeName = "b-tree leaf " + (last ? "(last)" : "");
                    break;
                case Page.TYPE_BTREE_NODE:
                    typeName = "b-tree node " + (last ? "(last)" : "");
                    break;
                case Page.TYPE_FREE_LIST:
                    typeName = "free list " + (last ? "(last)" : "");
                    break;
                case Page.TYPE_STREAM_TRUNK:
                    typeName = "log trunk";
                    break;
                case Page.TYPE_STREAM_DATA:
                    typeName = "log data";
                    break;
                default:
                    typeName = "ERROR: unknown type " + type;
                    break;
                }
                writer.println("-- undo page " + pageId + " " + typeName);
            } else if (x == PageLog.ADD) {
                int sessionId = in.readVarInt();
                setStorage(in.readVarInt());
                Row row = PageLog.readRow(in, s);
                writer.println("-- session " + sessionId +
                        " table " + storageId +
                        " add " + row.toString());
            } else if (x == PageLog.REMOVE) {
                int sessionId = in.readVarInt();
                setStorage(in.readVarInt());
                long key = in.readVarLong();
                writer.println("-- session " + sessionId +
                        " table " + storageId +
                        " remove " + key);
            } else if (x == PageLog.TRUNCATE) {
                int sessionId = in.readVarInt();
                setStorage(in.readVarInt());
                writer.println("-- session " + sessionId +
                        " table " + storageId +
                        " truncate");
            } else if (x == PageLog.COMMIT) {
                int sessionId = in.readVarInt();
                writer.println("-- commit " + sessionId);
            } else if (x == PageLog.ROLLBACK) {
                int sessionId = in.readVarInt();
                writer.println("-- rollback " + sessionId);
            } else if (x == PageLog.PREPARE_COMMIT) {
                int sessionId = in.readVarInt();
                String transaction = in.readString();
                writer.println("-- prepare commit " + sessionId + " " + transaction);
            } else if (x == PageLog.NOOP) {
                // nothing to do
            } else if (x == PageLog.CHECKPOINT) {
                writer.println("-- checkpoint");
            } else if (x == PageLog.FREE_LOG) {
                int size = in.readVarInt();
                StringBuilder buff = new StringBuilder("-- free");
                for (int i = 0; i < size; i++) {
                    buff.append(' ').append(in.readVarInt());
                }
                writer.println(buff);
            } else {
                writer.println("-- ERROR: unknown operation " + x);
                break;
            }
        }
    }

    private String setStorage(int storageId) {
        this.storageId = storageId;
        this.storageName = "O_" + String.valueOf(storageId).replace('-', 'M');
        return storageName;
    }

    /**
     * An input stream that reads the data from a page store.
     */
    static class PageInputStream extends InputStream {

        private final PrintWriter writer;
        private final FileStore store;
        private final Data page;
        private final int pageSize;
        private int trunkPage;
        private int dataPage;
        private IntArray dataPages = new IntArray();
        private boolean endOfFile;
        private int remaining;
        private int logKey;

        public PageInputStream(PrintWriter writer, DataHandler handler,
                FileStore store, int logKey, int firstTrunkPage, int firstDataPage, int pageSize) {
            this.writer = writer;
            this.store = store;
            this.pageSize = pageSize;
            this.logKey = logKey - 1;
            this.trunkPage = firstTrunkPage;
            this.dataPage = firstDataPage;
            page = Data.create(handler, pageSize);
        }

        public int read() {
            byte[] b = { 0 };
            int len = read(b);
            return len < 0 ? -1 : (b[0] & 255);
        }

        public int read(byte[] b) {
            return read(b, 0, b.length);
        }

        public int read(byte[] b, int off, int len) {
            if (len == 0) {
                return 0;
            }
            int read = 0;
            while (len > 0) {
                int r = readBlock(b, off, len);
                if (r < 0) {
                    break;
                }
                read += r;
                off += r;
                len -= r;
            }
            return read == 0 ? -1 : read;
        }

        private int readBlock(byte[] buff, int off, int len) {
            fillBuffer();
            if (endOfFile) {
                return -1;
            }
            int l = Math.min(remaining, len);
            page.read(buff, off, l);
            remaining -= l;
            return l;
        }

        private void fillBuffer() {
            if (remaining > 0 || endOfFile) {
                return;
            }
            while (dataPages.size() == 0) {
                if (trunkPage == 0) {
                    endOfFile = true;
                    return;
                }
                store.seek((long) trunkPage * pageSize);
                store.readFully(page.getBytes(), 0, pageSize);
                page.reset();
                if (!PageStore.checksumTest(page.getBytes(), trunkPage, pageSize)) {
                    writer.println("-- ERROR: checksum mismatch page: " +trunkPage);
                    endOfFile = true;
                    return;
                }
                int t = page.readByte();
                page.readShortInt();
                if (t != Page.TYPE_STREAM_TRUNK) {
                    writer.println("-- eof  page: " + trunkPage + " type: " + t + " expected type: " + Page.TYPE_STREAM_TRUNK);
                    endOfFile = true;
                    return;
                }
                page.readInt();
                int key = page.readInt();
                logKey++;
                if (key != logKey) {
                    writer.println("-- eof  page: " + trunkPage + " type: " + t + " expected key: " + logKey + " got: " + key);
                }
                trunkPage = page.readInt();
                int pageCount = page.readShortInt();
                for (int i = 0; i < pageCount; i++) {
                    int d = page.readInt();
                    if (dataPage != 0) {
                        if (d == dataPage) {
                            dataPage = 0;
                        } else {
                            // ignore the pages before the starting page
                            continue;
                        }
                    }
                    dataPages.add(d);
                }
            }
            if (dataPages.size() > 0) {
                page.reset();
                int nextPage = dataPages.get(0);
                dataPages.remove(0);
                store.seek((long) nextPage * pageSize);
                store.readFully(page.getBytes(), 0, pageSize);
                page.reset();
                int t = page.readByte();
                if (t != 0 && !PageStore.checksumTest(page.getBytes(), nextPage, pageSize)) {
                    writer.println("-- ERROR: checksum mismatch page: " +nextPage);
                    endOfFile = true;
                    return;
                }
                page.readShortInt();
                int p = page.readInt();
                int k = page.readInt();
                if (t != Page.TYPE_STREAM_DATA) {
                    writer.println("-- eof  page: " +nextPage+ " type: " + t + " parent: " + p +
                            " expected type: " + Page.TYPE_STREAM_DATA);
                    endOfFile = true;
                    return;
                } else if (k != logKey) {
                    writer.println("-- eof  page: " +nextPage+ " type: " + t + " parent: " + p +
                            " expected key: " + logKey + " got: " + k);
                    endOfFile = true;
                    return;
                }
                remaining = pageSize - page.length();
            }
        }
    }

    private void dumpPageBtreeNode(PrintWriter writer, Data s, int pageId, boolean positionOnly) {
        int rowCount = s.readInt();
        int entryCount = s.readShortInt();
        int[] children = new int[entryCount + 1];
        int[] offsets = new int[entryCount];
        children[entryCount] = s.readInt();
        checkParent(writer, pageId, children, entryCount);
        int empty = Integer.MAX_VALUE;
        for (int i = 0; i < entryCount; i++) {
            children[i] = s.readInt();
            checkParent(writer, pageId, children, i);
            int off = s.readShortInt();
            empty = Math.min(off, empty);
            offsets[i] = off;
        }
        empty = empty - s.length();
        if (!trace) {
            return;
        }
        writer.println("--   empty: " + empty);
        for (int i = 0; i < entryCount; i++) {
            int off = offsets[i];
            s.setPos(off);
            long key = s.readVarLong();
            Value data;
            if (positionOnly) {
                data = ValueLong.get(key);
            } else {
                try {
                    data = s.readValue();
                } catch (Throwable e) {
                    writeDataError(writer, "exception " + e, s.getBytes());
                    continue;
                }
            }
            writer.println("-- [" + i + "] child: " + children[i] + " key: " + key + " data: " + data);
        }
        writer.println("-- [" + entryCount + "] child: " + children[entryCount] + " rowCount: " + rowCount);
    }

    private int dumpPageFreeList(PrintWriter writer, Data s, long pageId, long pageCount) {
        int pagesAddressed = PageFreeList.getPagesAddressed(pageSize);
        BitSet used = new BitSet();
        for (int i = 0; i < pagesAddressed; i += 8) {
            int x = s.readByte() & 255;
            for (int j = 0; j < 8; j++) {
                if ((x & (1 << j)) != 0) {
                    used.set(i + j);
                }
            }
        }
        int free = 0;
        for (long i = 0, j = pageId; i < pagesAddressed && j < pageCount; i++, j++) {
            if (i == 0 || j % 100 == 0) {
                if (i > 0) {
                    writer.println();
                }
                writer.print("-- " + j + " ");
            } else if (j % 20 == 0) {
                writer.print(" - ");
            } else if (j % 10 == 0) {
                writer.print(' ');
            }
            writer.print(used.get((int) i) ? '1' : '0');
            if (!used.get((int) i)) {
                free++;
            }
        }
        writer.println();
        return free;
    }

    private void dumpPageBtreeLeaf(PrintWriter writer, Data s, int entryCount, boolean positionOnly) {
        int[] offsets = new int[entryCount];
        int empty = Integer.MAX_VALUE;
        for (int i = 0; i < entryCount; i++) {
            int off = s.readShortInt();
            empty = Math.min(off, empty);
            offsets[i] = off;
        }
        empty = empty - s.length();
        writer.println("--   empty: " + empty);
        for (int i = 0; i < entryCount; i++) {
            int off = offsets[i];
            s.setPos(off);
            long key = s.readVarLong();
            Value data;
            if (positionOnly) {
                data = ValueLong.get(key);
            } else {
                try {
                    data = s.readValue();
                } catch (Throwable e) {
                    writeDataError(writer, "exception " + e, s.getBytes());
                    continue;
                }
            }
            writer.println("-- [" + i + "] key: " + key + " data: " + data);
        }
    }

    private void checkParent(PrintWriter writer, long pageId, int[] children, int index) {
        int child = children[index];
        if (child < 0 || child >= parents.length) {
            writer.println("-- ERROR [" + pageId + "] child[" + index + "]: " + child + " >= page count: " + parents.length);
        } else if (parents[child] != pageId) {
            writer.println("-- ERROR [" + pageId + "] child[" + index + "]: " + child + " parent: " + parents[child]);
        }
    }

    private void dumpPageDataNode(PrintWriter writer, Data s, long pageId, int entryCount) {
        int[] children = new int[entryCount + 1];
        long[] keys = new long[entryCount];
        children[entryCount] = s.readInt();
        checkParent(writer, pageId, children, entryCount);
        for (int i = 0; i < entryCount; i++) {
            children[i] = s.readInt();
            checkParent(writer, pageId, children, i);
            keys[i] = s.readVarLong();
        }
        if (!trace) {
            return;
        }
        for (int i = 0; i < entryCount; i++) {
            writer.println("-- [" + i + "] child: " + children[i] + " key: " + keys[i]);
        }
        writer.println("-- [" + entryCount + "] child: " + children[entryCount]);
    }

    private void dumpPageDataLeaf(PrintWriter writer, Data s, boolean last, long pageId, int columnCount, int entryCount) {
        long[] keys = new long[entryCount];
        int[] offsets = new int[entryCount];
        long next = 0;
        if (!last) {
            next = s.readInt();
            writer.println("--   next: " + next);
        }
        int empty = pageSize;
        for (int i = 0; i < entryCount; i++) {
            keys[i] = s.readVarLong();
            int off = s.readShortInt();
            empty = Math.min(off, empty);
            offsets[i] = off;
        }
        stat.pageDataRows += pageSize - empty;
        empty = empty - s.length();
        stat.pageDataHead += s.length();
        stat.pageDataEmpty += empty;
        if (trace) {
            writer.println("--   empty: " + empty);
        }
        if (!last) {
            Data s2 = Data.create(this, pageSize);
            s.setPos(pageSize);
            long parent = pageId;
            while (true) {
                checkParent(writer, parent, new int[]{(int) next}, 0);
                parent = next;
                store.seek(pageSize * next);
                store.readFully(s2.getBytes(), 0, pageSize);
                s2.reset();
                int type = s2.readByte();
                s2.readShortInt();
                s2.readInt();
                if (type == (Page.TYPE_DATA_OVERFLOW | Page.FLAG_LAST)) {
                    int size = s2.readShortInt();
                    writer.println("-- chain: " + next + " type: " + type + " size: " + size);
                    s.checkCapacity(size);
                    s.write(s2.getBytes(), s2.length(), size);
                    break;
                } else if (type == Page.TYPE_DATA_OVERFLOW) {
                    next = s2.readInt();
                    if (next == 0) {
                        writeDataError(writer, "next:0", s2.getBytes());
                        break;
                    }
                    int size = pageSize - s2.length();
                    writer.println("-- chain: " + next + " type: " + type + " size: " + size + " next: " + next);
                    s.checkCapacity(size);
                    s.write(s2.getBytes(), s2.length(), size);
                } else {
                    writeDataError(writer, "type: " + type, s2.getBytes());
                    break;
                }
            }
        }
        for (int i = 0; i < entryCount; i++) {
            long key = keys[i];
            int off = offsets[i];
            if (trace) {
                writer.println("-- [" + i + "] storage: " + storageId + " key: " + key + " off: " + off);
            }
            s.setPos(off);
            Value[] data = createRecord(writer, s, columnCount);
            if (data != null) {
                createTemporaryTable(writer);
                writeRow(writer, s, data);
                if (remove && storageId == 0) {
                    String sql = data[3].getString();
                    if (sql.startsWith("CREATE USER ")) {
                        int saltIndex = Utils.indexOf(s.getBytes(), "SALT ".getBytes(), off);
                        if (saltIndex >= 0) {
                            String userName = sql.substring("CREATE USER ".length(), sql.indexOf("SALT ") - 1);
                            if (userName.startsWith("\"")) {
                                // TODO doesn't work for all cases ("" inside user name)
                                userName = userName.substring(1, userName.length() - 1);
                            }
                            SHA256 sha = new SHA256();
                            byte[] userPasswordHash = sha.getKeyPasswordHash(userName, "".toCharArray());
                            byte[] salt = MathUtils.secureRandomBytes(Constants.SALT_LEN);
                            byte[] passwordHash = sha.getHashWithSalt(userPasswordHash, salt);
                            StringBuilder buff = new StringBuilder();
                            buff.append("SALT '").
                                append(Utils.convertBytesToString(salt)).
                                append("' HASH '").
                                append(Utils.convertBytesToString(passwordHash)).
                                append('\'');
                            byte[] replacement = buff.toString().getBytes();
                            System.arraycopy(replacement, 0, s.getBytes(), saltIndex, replacement.length);
                            store.seek(pageSize * pageId);
                            store.write(s.getBytes(), 0, pageSize);
                            if (trace) {
                                out.println("User: " + userName);
                            }
                            remove = false;
                        }
                    }
                }
            }
        }
    }

    private Value[] createRecord(PrintWriter writer, Data s, int columnCount) {
        recordLength = columnCount;
        if (columnCount <= 0) {
            writeDataError(writer, "columnCount<0", s.getBytes());
            return null;
        }
        Value[] data;
        try {
            data = new Value[columnCount];
        } catch (OutOfMemoryError e) {
            writeDataError(writer, "out of memory", s.getBytes());
            return null;
        }
        return data;
    }

    private void writeRow(PrintWriter writer, Data s, Value[] data) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO " + storageName + " VALUES(");
        for (valueId = 0; valueId < recordLength; valueId++) {
            try {
                Value v = s.readValue();
                data[valueId] = v;
                if (valueId > 0) {
                    sb.append(", ");
                }
                String columnName = storageName + "." + valueId;
                sb.append(getSQL(columnName, v));
            } catch (Exception e) {
                writeDataError(writer, "exception " + e, s.getBytes());
                continue;
            } catch (OutOfMemoryError e) {
                writeDataError(writer, "out of memory", s.getBytes());
                continue;
            }
        }
        sb.append(");");
        writer.println(sb.toString());
        if (storageId == 0) {
            try {
                SimpleRow r = new SimpleRow(data);
                MetaRecord meta = new MetaRecord(r);
                schema.add(meta);
                if (meta.getObjectType() == DbObject.TABLE_OR_VIEW) {
                    String sql = data[3].getString();
                    String name = extractTableOrViewName(sql);
                    tableMap.put(meta.getId(), name);
                }
            } catch (Throwable t) {
                writeError(writer, t);
            }
        }
    }

    private void resetSchema() {
        schema = New.arrayList();
        objectIdSet = New.hashSet();
        tableMap = New.hashMap();
        columnTypeMap = New.hashMap();
    }

    private void writeSchema(PrintWriter writer) {
        writer.println("---- Schema ----------");
        Collections.sort(schema);
        for (MetaRecord m : schema) {
            String sql = m.getSQL();
            // create, but not referential integrity constraints and so on
            if (sql.startsWith("CREATE ")) {
                writer.println(sql + ";");
            }
        }
        // first, copy the lob storage (if there is any)
        // must occur before copying data,
        // otherwise the lob storage may be overwritten
        boolean deleteLobs = false;
        for (Map.Entry<Integer, String> entry : tableMap.entrySet()) {
            Integer objectId = entry.getKey();
            String name = entry.getValue();
            if (objectIdSet.contains(objectId)) {
                if (name.startsWith("INFORMATION_SCHEMA.LOB")) {
                    setStorage(objectId);
                    writer.println("DELETE FROM " + name + ";");
                    writer.println("INSERT INTO " + name + " SELECT * FROM " + storageName + ";");
                    if (name.startsWith("INFORMATION_SCHEMA.LOBS")) {
                        writer.println("UPDATE " + name + " SET TABLE = " + LobStorage.TABLE_TEMP + ";");
                        deleteLobs = true;
                    }
                }
            }
        }
        for (Map.Entry<Integer, String> entry : tableMap.entrySet()) {
            Integer objectId = entry.getKey();
            String name = entry.getValue();
            if (objectIdSet.contains(objectId)) {
                setStorage(objectId);
                if (name.startsWith("INFORMATION_SCHEMA.LOB")) {
                    continue;
                }
                writer.println("INSERT INTO " + name + " SELECT * FROM " + storageName + ";");
            }
        }
        for (Integer objectId : objectIdSet) {
            setStorage(objectId);
            writer.println("DROP TABLE " + storageName + ";");
        }
        writer.println("DROP ALIAS READ_BLOB;");
        writer.println("DROP ALIAS READ_CLOB;");
        writer.println("DROP ALIAS READ_BLOB_DB;");
        writer.println("DROP ALIAS READ_CLOB_DB;");
        if (deleteLobs) {
            writer.println("DELETE FROM INFORMATION_SCHEMA.LOBS WHERE TABLE = " + LobStorage.TABLE_TEMP + ";");
        }
        for (MetaRecord m : schema) {
            String sql = m.getSQL();
            // everything except create
            if (!sql.startsWith("CREATE ")) {
                writer.println(sql + ";");
            }
        }
    }

    private void createTemporaryTable(PrintWriter writer) {
        if (!objectIdSet.contains(storageId)) {
            objectIdSet.add(storageId);
            StatementBuilder buff = new StatementBuilder("CREATE TABLE ");
            buff.append(storageName).append('(');
            for (int i = 0; i < recordLength; i++) {
                buff.appendExceptFirst(", ");
                buff.append('C').append(i).append(' ');
                String columnType = columnTypeMap.get(storageName + "." + i);
                if (columnType == null) {
                    buff.append("VARCHAR");
                } else {
                    buff.append(columnType);
                }
            }
            writer.println(buff.append(");").toString());
            writer.flush();
        }
    }

    private String extractTableOrViewName(String sql) {
        int indexTable = sql.indexOf(" TABLE ");
        int indexView = sql.indexOf(" VIEW ");
        if (indexTable > 0 && indexView > 0) {
            if (indexTable < indexView) {
                indexView = -1;
            } else {
                indexTable = -1;
            }
        }
        if (indexView > 0) {
            sql = sql.substring(indexView + " VIEW ".length());
        } else if (indexTable > 0) {
            sql = sql.substring(indexTable + " TABLE ".length());
        } else {
            return "UNKNOWN";
        }
        if (sql.startsWith("IF NOT EXISTS ")) {
            sql = sql.substring("IF NOT EXISTS ".length());
        }
        boolean ignore = false;
        for (int i = 0; i < sql.length(); i++) {
            char ch = sql.charAt(i);
            if (ch == '\"') {
                ignore = !ignore;
            } else if (!ignore && (ch <= ' ' || ch == '(')) {
                sql = sql.substring(0, i);
                return sql;
            }
        }
        return "UNKNOWN";
    }


    private void closeSilently(FileStore fileStore) {
        if (fileStore != null) {
            fileStore.closeSilently();
        }
    }

    private void writeError(PrintWriter writer, Throwable e) {
        if (writer != null) {
            writer.println("// error: " + e);
        }
        traceError("Error", e);
    }

    /**
     * INTERNAL
     */
    public String getDatabasePath() {
        return databaseName;
    }

    /**
     * INTERNAL
     */
    public FileStore openFile(String name, String mode, boolean mustExist) {
        return FileStore.open(this, name, "rw");
    }

    /**
     * INTERNAL
     */
    public void checkPowerOff() {
        // nothing to do
    }

    /**
     * INTERNAL
     */
    public void checkWritingAllowed() {
        // nothing to do
    }

    /**
     * INTERNAL
     */
    public void freeUpDiskSpace() {
        // nothing to do
    }

    /**
     * INTERNAL
     */
    public int getMaxLengthInplaceLob() {
        throw DbException.throwInternalError();
    }

    /**
     * INTERNAL
     */
    public String getLobCompressionAlgorithm(int type) {
        return null;
    }

    /**
     * INTERNAL
     */
    public Object getLobSyncObject() {
        return this;
    }

    /**
     * INTERNAL
     */
    public SmallLRUCache<String, String[]> getLobFileListCache() {
        return null;
    }

    /**
     * INTERNAL
     */
    public TempFileDeleter getTempFileDeleter() {
        return TempFileDeleter.getInstance();
    }

    /**
     * INTERNAL
     */
    public LobStorage getLobStorage() {
        return null;
    }

    /**
     * INTERNAL
     */
    public Connection getLobConnection() {
        return null;
    }

}
