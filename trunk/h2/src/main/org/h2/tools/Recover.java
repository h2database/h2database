/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.h2.command.Parser;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.DbObject;
import org.h2.engine.MetaRecord;
import org.h2.index.Page;
import org.h2.log.LogFile;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.result.Row;
import org.h2.result.SimpleRow;
import org.h2.security.SHA256;
import org.h2.store.DataHandler;
import org.h2.store.DataPage;
import org.h2.store.DiskFile;
import org.h2.store.FileLister;
import org.h2.store.FileStore;
import org.h2.store.FileStoreInputStream;
import org.h2.store.PageLog;
import org.h2.store.PageStore;
import org.h2.util.ByteUtils;
import org.h2.util.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.ObjectArray;
import org.h2.util.ObjectUtils;
import org.h2.util.RandomUtils;
import org.h2.util.SmallLRUCache;
import org.h2.util.TempFileDeleter;
import org.h2.util.Tool;
import org.h2.value.Value;
import org.h2.value.ValueLob;

/**
 * Dumps the contents of a database file to a human readable text file. This
 * text file can be used to recover most of the data. This tool does not open
 * the database and can be used even if the database files are corrupted. A
 * database can get corrupted if there is a bug in the database engine or file
 * system software, or if an application writes into the database file that
 * doesn't understand the the file format, or if there is a hardware problem.
 */
public class Recover extends Tool implements DataHandler {

    private String databaseName;
    private int block;
    private int blockCount;
    private int storageId;
    private int recordLength;
    private int valueId;
    private boolean trace;
    private boolean lobFilesInDirectories;
    private ObjectArray schema;
    private HashSet objectIdSet;
    private HashMap tableMap;
    private boolean remove;

    private void showUsage() {
        out.println("Helps recovering a corrupted database.");
        out.println("java "+getClass().getName() + "\n" +
                " [-dir <dir>]      The directory (default: .)\n" +
                " [-db <database>]  The database name\n" +
                " [-trace]          Print additional trace information");
        out.println("See also http://h2database.com/javadoc/" + getClass().getName().replace('.', '/') + ".html");
    }

    /**
     * The command line interface for this tool.
     * The options must be split into strings like this: "-db", "test",...
     * Options are case sensitive. The following options are supported:
     * <ul>
     * <li>-help or -? (print the list of options)
     * </li><li>-dir database directory (the default is the current directory)
     * </li><li>-db database name (all databases if no name is specified)
     * </li><li>-trace (print additional trace information while processing)
     * </li></ul>
     *
     * @param args the command line arguments
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        new Recover().run(args);
    }

    public void run(String[] args) throws SQLException {
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
                out.println("Unsupported option: " + arg);
                showUsage();
                return;
            }
        }
        if (!SysProperties.PAGE_STORE && remove) {
            removePassword(dir, db);
        } else {
            process(dir, db);
        }
    }

    /**
     * INTERNAL
     */
    public static Reader readClob(String fileName) throws IOException {
        return new BufferedReader(new InputStreamReader(readBlob(fileName)));
    }

    /**
     * INTERNAL
     */
    public static InputStream readBlob(String fileName) throws IOException {
        return new BufferedInputStream(new FileInputStream(fileName));
    }

    private void removePassword(String dir, String db) throws SQLException {
        ArrayList list = FileLister.getDatabaseFiles(dir, db, true);
        if (list.size() == 0) {
            printNoDatabaseFilesFound(dir, db);
        }
        for (int i = 0; i < list.size(); i++) {
            String fileName = (String) list.get(i);
            if (fileName.endsWith(Constants.SUFFIX_DATA_FILE)) {
                removePassword(fileName);
            }
        }
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

    private void removePassword(String fileName) throws SQLException {
        if (fileName.endsWith(Constants.SUFFIX_PAGE_FILE)) {
            remove = true;
            dumpPageStore(fileName);
            return;
        }
        setDatabaseName(fileName.substring(fileName.length() - Constants.SUFFIX_DATA_FILE.length()));
        FileStore store = FileStore.open(null, fileName, "rw");
        long length = store.length();
        int offset = FileStore.HEADER_LENGTH;
        int blockSize = DiskFile.BLOCK_SIZE;
        int blocks = (int) (length / blockSize);
        blockCount = 1;
        for (int block = 0; block < blocks; block += blockCount) {
            store.seek(offset + (long) block * blockSize);
            byte[] bytes = new byte[blockSize];
            DataPage s = DataPage.create(this, bytes);
            long start = store.getFilePointer();
            store.readFully(bytes, 0, blockSize);
            blockCount = s.readInt();
            storageId = -1;
            recordLength = -1;
            valueId = -1;
            if (blockCount == 0) {
                // free block
                blockCount = 1;
                continue;
            } else if (blockCount < 0) {
                blockCount = 1;
                continue;
            }
            try {
                s.checkCapacity(blockCount * blockSize);
            } catch (OutOfMemoryError e) {
                blockCount = 1;
                continue;
            }
            if (blockCount > 1) {
                store.readFully(s.getBytes(), blockSize, blockCount * blockSize - blockSize);
            }
            try {
                s.check(blockCount * blockSize);
            } catch (SQLException e) {
                blockCount = 1;
                continue;
            }
            storageId = s.readInt();
            if (storageId != 0) {
                continue;
            }
            recordLength = s.readInt();
            if (recordLength <= 0) {
                continue;
            }
            Value[] data;
            try {
                data = new Value[recordLength];
            } catch (Throwable e) {
                continue;
            }
            for (valueId = 0; valueId < recordLength; valueId++) {
                try {
                    data[valueId] = s.readValue();
                } catch (Throwable e) {
                    continue;
                }
            }
            if (storageId == 0) {
                try {
                    String sql = data[3].getString();
                    if (!sql.startsWith("CREATE USER ")) {
                        continue;
                    }
                    int idx = sql.indexOf("SALT");
                    if (idx < 0) {
                        continue;
                    }
                    String userName = sql.substring("CREATE USER ".length(), idx - 1);
                    if (userName.startsWith("\"")) {
                        // TODO doesn't work for all cases ("" inside user name)
                        userName = userName.substring(1, userName.length() - 1);
                    }
                    SHA256 sha = new SHA256();
                    byte[] userPasswordHash = sha.getKeyPasswordHash(userName, "".toCharArray());
                    byte[] salt = RandomUtils.getSecureBytes(Constants.SALT_LEN);
                    byte[] passwordHash = sha.getHashWithSalt(userPasswordHash, salt);
                    boolean admin = sql.indexOf("ADMIN") >= 0;
                    StringBuffer buff = new StringBuffer();
                    buff.append("CREATE USER ");
                    buff.append(Parser.quoteIdentifier(userName));
                    buff.append(" SALT '");
                    buff.append(ByteUtils.convertBytesToString(salt));
                    buff.append("' HASH '");
                    buff.append(ByteUtils.convertBytesToString(passwordHash));
                    buff.append("'");
                    if (admin) {
                        buff.append(" ADMIN");
                    }
                    byte[] replacement = buff.toString().getBytes();
                    int at = ByteUtils.indexOf(s.getBytes(), "CREATE USER ".getBytes(), 0);
                    System.arraycopy(replacement, 0, s.getBytes(), at, replacement.length);
                    s.fill(blockCount * blockSize);
                    s.updateChecksum();
                    store.seek(start);
                    store.write(s.getBytes(), 0, s.length());
                    if (trace) {
                        out.println("User: " + userName);
                    }
                    break;
                } catch (Throwable e) {
                    e.printStackTrace(out);
                }
            }
        }
        closeSilently(store);
    }

    /**
     * Dumps the database.
     *
     * @param dir the directory
     * @param db the database name (null for all databases)
     * @throws SQLException
     */
    public static void execute(String dir, String db) throws SQLException {
        new Recover().process(dir, db);
    }

    private void process(String dir, String db) throws SQLException {
        ArrayList list = FileLister.getDatabaseFiles(dir, db, true);
        if (list.size() == 0) {
            printNoDatabaseFilesFound(dir, db);
        }
        for (int i = 0; i < list.size(); i++) {
            String fileName = (String) list.get(i);
            if (fileName.endsWith(Constants.SUFFIX_DATA_FILE)) {
                dumpData(fileName);
            } else if (fileName.endsWith(Constants.SUFFIX_PAGE_FILE)) {
                dumpPageStore(fileName);
            } else if (fileName.endsWith(Constants.SUFFIX_INDEX_FILE)) {
                dumpIndex(fileName);
            } else if (fileName.endsWith(Constants.SUFFIX_LOG_FILE)) {
                dumpLog(fileName);
            } else if (fileName.endsWith(Constants.SUFFIX_LOB_FILE)) {
                dumpLob(fileName, true);
                dumpLob(fileName, false);
            }
        }
    }

    private PrintWriter getWriter(String fileName, String suffix) throws SQLException {
        fileName = fileName.substring(0, fileName.length() - 3);
        String outputFile = fileName + suffix;
        trace("Created file: " + outputFile);
        return new PrintWriter(IOUtils.getWriter(FileUtils.openFileOutputStream(outputFile, false)));
    }

    private void writeDataError(PrintWriter writer, String error, byte[] data, int dumpBlocks) {
        writer.println("-- ERROR: " + error + " block:" + block + " blockCount:" + blockCount + " storageId:"
                + storageId + " recordLength:" + recordLength + " valueId:" + valueId);
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < dumpBlocks * DiskFile.BLOCK_SIZE; i++) {
            int x = data[i] & 0xff;
            if (x >= ' ' && x < 128) {
                sb.append((char) x);
            } else {
                sb.append('?');
            }
        }
        writer.println("-- dump: " + sb.toString());
        sb = new StringBuffer();
        for (int i = 0; i < dumpBlocks * DiskFile.BLOCK_SIZE; i++) {
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
        FileStore store = null;
        int size = 0;
        String n = fileName + (lobCompression ? ".comp" : "") + ".txt";
        InputStream in = null;
        try {
            fileOut = FileUtils.openFileOutputStream(n, false);
            store = FileStore.open(null, fileName, "r");
            store.init();
            in = new BufferedInputStream(new FileStoreInputStream(store, this, lobCompression, false));
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
            if (trace) {
                traceError(fileName, e);
            }
        } finally {
            IOUtils.closeSilently(fileOut);
            IOUtils.closeSilently(in);
            closeSilently(store);
        }
        if (size == 0) {
            try {
                FileUtils.delete(n);
            } catch (SQLException e) {
                traceError(n, e);
            }
        }
    }

    private void writeLogRecord(PrintWriter writer, DataPage s) {
        recordLength = s.readInt();
        if (recordLength <= 0) {
            writeDataError(writer, "recordLength<0", s.getBytes(), blockCount);
            return;
        }
        Value[] data;
        try {
            data = new Value[recordLength];
        } catch (OutOfMemoryError e) {
            writeDataError(writer, "out of memory", s.getBytes(), blockCount);
            return;
        }
        StringBuffer sb = new StringBuffer();
        sb.append("//     data: ");
        for (valueId = 0; valueId < recordLength; valueId++) {
            try {
                Value v = s.readValue();
                data[valueId] = v;
                if (valueId > 0) {
                    sb.append(", ");
                }
                sb.append(getSQL(v));
            } catch (Exception e) {
                if (trace) {
                    traceError("log data", e);
                }
                writeDataError(writer, "exception " + e, s.getBytes(), blockCount);
                continue;
            } catch (OutOfMemoryError e) {
                writeDataError(writer, "out of memory", s.getBytes(), blockCount);
                continue;
            }
        }
        writer.println(sb.toString());
        writer.flush();
    }

    private String getSQL(Value v) {
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
        }
        return v.getSQL();
    }

    private void setDatabaseName(String name) {
        databaseName = name;
        lobFilesInDirectories = FileUtils.exists(databaseName + Constants.SUFFIX_LOBS_DIRECTORY);
    }

    private void dumpLog(String fileName) {
        PrintWriter writer = null;
        FileStore store = null;
        try {
            setDatabaseName(fileName.substring(fileName.length() - Constants.SUFFIX_LOG_FILE.length()));
            writer = getWriter(fileName, ".txt");
            store = FileStore.open(null, fileName, "r");
            long length = store.length();
            writer.println("// length: " + length);
            int offset = FileStore.HEADER_LENGTH;
            int blockSize = LogFile.BLOCK_SIZE;
            int blocks = (int) (length / blockSize);
            byte[] buff = new byte[blockSize];
            DataPage s = DataPage.create(this, buff);
            s.fill(3 * blockSize);
            int len = s.length();
            s.reset();
            if (length < FileStore.HEADER_LENGTH + len) {
                // this is an empty file
                writer.println("// empty file");
                return;
            }
            store.seek(offset);
            store.readFully(s.getBytes(), 0, len);
            int id = s.readInt();
            int firstUncommittedPos = s.readInt();
            int firstUnwrittenPos = s.readInt();
            writer.println("// id:" + id);
            writer.println("// firstUncommittedPos:" + firstUncommittedPos);
            writer.println("// firstUnwrittenPos:" + firstUnwrittenPos);
            int max = (int) (length / blockSize);
            writer.println("// max:" + max);
            while (true) {
                int pos = (int) (store.getFilePointer() / blockSize);
                if ((long) pos * blockSize >= length) {
                    break;
                }
                buff = new byte[blockSize];
                store.readFully(buff, 0, blockSize);
                s = DataPage.create(this, buff);
                // Math.abs(Integer.MIN_VALUE) == Integer.MIN_VALUE
                blocks = MathUtils.convertLongToInt(Math.abs(s.readInt()));
                if (blocks > 1) {
                    byte[] b2 = ByteUtils.newBytes(blocks * blockSize);
                    System.arraycopy(buff, 0, b2, 0, blockSize);
                    buff = b2;
                    try {
                        store.readFully(buff, blockSize, blocks * blockSize - blockSize);
                    } catch (SQLException e) {
                        break;
                    }
                    s = DataPage.create(this, buff);
                    s.check(blocks * blockSize);
                }
                s.reset();
                // Math.abs(Integer.MIN_VALUE) == Integer.MIN_VALUE
                blocks = MathUtils.convertLongToInt(Math.abs(s.readInt()));
                if (blocks == 0) {
                    writer.println("// [" + pos + "] blocks: " + blocks + " (end)");
                    break;
                }
                char type = (char) s.readByte();
                int sessionId = s.readInt();
                if (type == 'P') {
                    String transaction = s.readString();
                    writer.println("//   prepared session:" + sessionId + " tx:" + transaction);
                } else if (type == 'C') {
                    writer.println("//   commit session:" + sessionId);
                } else {
                    int storageId = s.readInt();
                    int recId = s.readInt();
                    int blockCount = s.readInt();
                    if (type != 'T') {
                        s.readDataPageNoSize();
                    }
                    switch (type) {
                    case 'S': {
                        char fileType = (char) s.readByte();
                        int sumLength = s.readInt();
                        byte[] summary = ByteUtils.newBytes(sumLength);
                        if (sumLength > 0) {
                            s.read(summary, 0, sumLength);
                        }
                        writer.println("//   summary session:"+sessionId+" fileType:" + fileType + " sumLength:" + sumLength);
                        dumpSummary(writer, summary);
                        break;
                    }
                    case 'T':
                        writer.println("//   truncate session:"+sessionId+" storage:" + storageId + " pos:" + recId + " blockCount:"+blockCount);
                        break;
                    case 'I':
                        writer.println("//   insert session:"+sessionId+" storage:" + storageId + " pos:" + recId + " blockCount:"+blockCount);
                        if (storageId >= 0) {
                            writeLogRecord(writer, s);
                        }
                        break;
                    case 'D':
                        writer.println("//   delete session:"+sessionId+" storage:" + storageId + " pos:" + recId + " blockCount:"+blockCount);
                        if (storageId >= 0) {
                            writeLogRecord(writer, s);
                        }
                        break;
                    default:
                        writer.println("//   type?:"+type+" session:"+sessionId+" storage:" + storageId + " pos:" + recId + " blockCount:"+blockCount);
                        break;
                    }
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

    private void dumpSummary(PrintWriter writer, byte[] summary) {
        if (summary == null || summary.length == 0) {
            writer.println("//     summary is empty");
            return;
        }
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(summary));
            int b2 = in.readInt();
            for (int i = 0; i < b2 / 8; i++) {
                int x = in.read();
                if ((i % 8) == 0) {
                    writer.print("//  ");
                }
                writer.print(" " + Long.toString(i * 8) + ":");
                for (int j = 0; j < 8; j++) {
                    writer.print(((x & 1) == 1) ? "1" : "0");
                    x >>>= 1;
                }
                if ((i % 8) == 7) {
                    writer.println("");
                }
            }
            writer.println("//");
            int len = in.readInt();
            for (int i = 0; i < len; i++) {
                int storageId = in.readInt();
                if (storageId != -1) {
                    writer.println("//     pos:" + (i * DiskFile.BLOCKS_PER_PAGE) + " storage:" + storageId);
                }
            }
            while (true) {
                int s = in.readInt();
                if (s < 0) {
                    break;
                }
                int recordCount = in.readInt();
                writer.println("//     storage:" + s + " recordCount:" + recordCount);
            }
        } catch (Throwable e) {
            writeError(writer, e);
        }
    }

    private void dumpIndex(String fileName) {
        PrintWriter writer = null;
        FileStore store = null;
        try {
            setDatabaseName(fileName.substring(fileName.length() - Constants.SUFFIX_INDEX_FILE.length()));
            writer = getWriter(fileName, ".txt");
            store = FileStore.open(null, fileName, "r");
            long length = store.length();
            int offset = FileStore.HEADER_LENGTH;
            int blockSize = DiskFile.BLOCK_SIZE;
            int blocks = (int) (length / blockSize);
            blockCount = 1;
            int[] pageOwners = new int[blocks / DiskFile.BLOCKS_PER_PAGE];
            for (block = 0; block < blocks; block += blockCount) {
                store.seek(offset + (long) block * blockSize);
                byte[] buff = new byte[blockSize];
                DataPage s = DataPage.create(this, buff);
                store.readFully(buff, 0, blockSize);
                blockCount = s.readInt();
                storageId = -1;
                recordLength = -1;
                valueId = -1;
                if (blockCount == 0) {
                    // free block
                    blockCount = 1;
                    continue;
                } else if (blockCount < 0) {
                    writeDataError(writer, "blockCount<0", s.getBytes(), 1);
                    blockCount = 1;
                    continue;
                } else if (((long) blockCount * blockSize) >= Integer.MAX_VALUE / 4) {
                    writeDataError(writer, "blockCount=" + blockCount, s.getBytes(), 1);
                    blockCount = 1;
                    continue;
                }
                try {
                    s.checkCapacity(blockCount * blockSize);
                } catch (OutOfMemoryError e) {
                    writeDataError(writer, "out of memory", s.getBytes(), 1);
                    blockCount = 1;
                    continue;
                }
                if (blockCount > 1) {
                    store.readFully(s.getBytes(), blockSize, blockCount * blockSize - blockSize);
                }
                try {
                    s.check(blockCount * blockSize);
                } catch (SQLException e) {
                    writeDataError(writer, "wrong checksum", s.getBytes(), 1);
                    blockCount = 1;
                    continue;
                }
                storageId = s.readInt();
                if (storageId < 0) {
                    writeDataError(writer, "storageId<0", s.getBytes(), blockCount);
                    continue;
                }
                int page = block / DiskFile.BLOCKS_PER_PAGE;
                if (pageOwners[page] != 0 && pageOwners[page] != storageId) {
                    writeDataError(writer, "double allocation, previous=" + pageOwners[page] + " now=" + storageId, s
                            .getBytes(), blockCount);
                } else {
                    pageOwners[page] = storageId;
                }
                writer.println("// [" + block + "] page:" + page + " blocks:" + blockCount + " storage:" + storageId);
            }
            writer.close();
        } catch (Throwable e) {
            writeError(writer, e);
            e.printStackTrace();
        } finally {
            IOUtils.closeSilently(writer);
            closeSilently(store);
        }
    }

    private void dumpPageStore(String fileName) {
        setDatabaseName(fileName.substring(0, fileName.length() - Constants.SUFFIX_PAGE_FILE.length()));
        FileStore store = null;
        PrintWriter writer = null;
        try {
            writer = getWriter(fileName, ".sql");
            writer.println("CREATE ALIAS IF NOT EXISTS READ_CLOB FOR \"" + this.getClass().getName() + ".readClob\";");
            writer.println("CREATE ALIAS IF NOT EXISTS READ_BLOB FOR \"" + this.getClass().getName() + ".readBlob\";");
            resetSchema();
            store = FileStore.open(null, fileName, remove ? "rw" : "r");
            long length = store.length();
            try {
                store.init();
            } catch (Exception e) {
                writeError(writer, e);
            }
            DataPage s = DataPage.create(this, 128);
            store.seek(0);
            store.readFully(s.getBytes(), 0, 128);
            s.setPos(48);
            int pageSize = s.readInt();
            int writeVersion = (int) s.readByte();
            int readVersion = (int) s.readByte();
            int systemTableRoot = s.readInt();
            int freeListHead = s.readInt();
            int logHead = s.readInt();
            writer.println("-- pageSize " + pageSize);
            writer.println("-- writeVersion: " + writeVersion);
            writer.println("-- readVersion: " + readVersion);
            writer.println("-- systemTableRoot: " + systemTableRoot);
            writer.println("-- freeListHead: " + freeListHead);
            writer.println("-- logHead: " + logHead);
            if (pageSize < PageStore.PAGE_SIZE_MIN || pageSize > PageStore.PAGE_SIZE_MAX) {
                pageSize = PageStore.PAGE_SIZE_DEFAULT;
                // use default values for other settings as well
                systemTableRoot = 1;
                freeListHead = 2;
                logHead = 3;
                writer.println("-- ERROR: page size; using " + pageSize);
            }
            int pageCount = (int) (length / pageSize);
            blockCount = 1;
            for (long page = 1; page < pageCount; page++) {
                s = DataPage.create(this, pageSize);
                store.seek(page * pageSize);
                store.readFully(s.getBytes(), 0, pageSize);
                int parentPageId = s.readInt();
                int type = s.readByte();
                switch (type) {
                case Page.TYPE_EMPTY:
                    // writer.println("-- page " + page + ": empty");
                    if (parentPageId != 0) {
                        writer.println("-- ERROR parent:" + parentPageId);
                    }
                    continue;
                }
                boolean last = (type & Page.FLAG_LAST) != 0;
                type &= ~Page.FLAG_LAST;
                switch (type) {
                case Page.TYPE_DATA_OVERFLOW:
                    writer.println("-- page " + page + ": data overflow " + (last ? "(last)" : ""));
                    break;
                case Page.TYPE_DATA_NODE:
                    writer.println("-- page " + page + ": data node " + (last ? "(last)" : ""));
                    break;
                case Page.TYPE_DATA_LEAF:
                    writer.println("-- page " + page + ": data leaf " + (last ? "(last)" : ""));
                    dumpPageDataLeaf(store, pageSize, writer, s, last, page);
                    break;
                case Page.TYPE_FREE_LIST:
                    writer.println("-- page " + page + ": free list " + (last ? "(last)" : ""));
                    break;
                case Page.TYPE_LOG:
                    writer.println("-- page " + page + ": log " + (last ? "(last)" : ""));
                    dumpPageLog(store, writer, s, last);
                    break;
                default:
                    writer.println("-- page " + page + ": ERROR unknown type " + type);
                    break;
                }
            }
            writeSchema(writer);
            DataInputStream in = new DataInputStream(
                    new PageInputStream(writer, this, store, logHead, pageSize, 0, Page.TYPE_LOG)
            );
            writer.println("-- log");
            while (true) {
                int x = in.read();
                if (x < 0) {
                    break;
                }
                if (x == PageLog.NO_OP) {
                    // nothing to do
                } else if (x == PageLog.UNDO) {
                    int pageId = in.readInt();
                    in.readFully(new byte[pageSize]);
                    writer.println("-- undo page " + pageId);
                } else if (x == PageLog.ADD || x == PageLog.REMOVE) {
                    int sessionId = in.readInt();
                    storageId = in.readInt();
                    Row row = PageLog.readRow(in, s);
                    writer.println("-- session " + sessionId +
                            " table " + storageId +
                            " " + (x == PageLog.ADD ? "add" : "remove") + " " + row.toString());
                } else if (x == PageLog.COMMIT) {
                    int sessionId = in.readInt();
                    writer.println("-- commit " + sessionId);
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

    /**
     * An input stream that reads the data from a page store.
     */
    static class PageInputStream extends InputStream {

        private final PrintWriter writer;
        private final int type;
        private final FileStore store;
        private final DataPage page;
        private final int pageSize;
        private int parentPage;
        private int nextPage;
        private boolean endOfFile;
        private int remaining;

        public PageInputStream(PrintWriter writer, DataHandler handler,
                FileStore store, int firstPage, int pageSize, int parent, int type) {
            this.writer = writer;
            this.store = store;
            this.pageSize = pageSize;
            this.type = type;
            this.parentPage = parent;
            nextPage = firstPage;
            page = DataPage.create(handler, pageSize);
        }

        public int read() throws IOException {
            byte[] b = new byte[1];
            int len = read(b);
            return len < 0 ? -1 : (b[0] & 255);
        }

        public int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        public int read(byte[] b, int off, int len) throws IOException {
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

        private int readBlock(byte[] buff, int off, int len) throws IOException {
            fillBuffer();
            if (endOfFile) {
                return -1;
            }
            int l = Math.min(remaining, len);
            page.read(buff, off, l);
            remaining -= l;
            return l;
        }

        private void fillBuffer() throws IOException {
            if (remaining > 0 || endOfFile) {
                return;
            }
            if (nextPage == 0) {
                endOfFile = true;
                return;
            }
            page.reset();
            try {
                store.seek((long) nextPage * pageSize);
                store.readFully(page.getBytes(), 0, pageSize);
                page.reset();
                int p = page.readInt();
                int t = page.readByte();
                boolean last = (t & Page.FLAG_LAST) != 0;
                t &= ~Page.FLAG_LAST;
                if (type != t || p != parentPage) {
                    writer.println("-- ERROR  page:" +nextPage+ " type:" + t + " parent:" + p +
                            " expected type:" + type + " expected parent:" + parentPage);
                }
                parentPage = nextPage;
                if (last) {
                    nextPage = 0;
                    remaining = page.readInt();
                } else {
                    nextPage = page.readInt();
                    remaining = pageSize - page.length();
                }
            } catch (SQLException e) {
                throw Message.convertToIOException(e);
            }
        }

    }

    private void dumpPageLog(FileStore store, PrintWriter writer, DataPage s, boolean last) {
        if (last) {
            int size = s.readInt();
            writer.println("--  size:" + size);
        } else {
            int next = s.readInt();
            writer.println("--  next:" + next);
        }
    }

    private void dumpPageDataLeaf(FileStore store, int pageSize, PrintWriter writer, DataPage s, boolean last, long pageId) throws SQLException {
        storageId = s.readInt();
        int entryCount = s.readShortInt();
        int[] keys = new int[entryCount];
        int[] offsets = new int[entryCount];
        long next = 0;
        if (!last) {
            next = s.readInt();
        }
        for (int i = 0; i < entryCount; i++) {
            keys[i] = s.readInt();
            offsets[i] = s.readShortInt();
        }
        if (!last) {
            DataPage s2 = DataPage.create(this, pageSize);
            s.setPos(pageSize);
            while (true) {
                store.seek(pageSize * next);
                store.readFully(s2.getBytes(), 0, pageSize);
                s2.setPos(4);
                int type = s2.readByte();
                if (type == (Page.TYPE_DATA_OVERFLOW | Page.FLAG_LAST)) {
                    int size = s2.readShortInt();
                    writer.println("-- chain:" + next + " type:" + type + " size:" + size);
                    s.write(s2.getBytes(), 7, size);
                    break;
                } else if (type == Page.TYPE_DATA_OVERFLOW) {
                    next = s2.readInt();
                    if (next == 0) {
                        writeDataError(writer, "next:0", s2.getBytes(), 1);
                        break;
                    }
                    int size = pageSize - 9;
                    writer.println("-- chain:" + next + " type:" + type + " size:" + size + " next:" + next);
                    s.write(s2.getBytes(), 9, size);
                } else {
                    writeDataError(writer, "type:" + type, s2.getBytes(), 1);
                    break;
                }
            }
        }
        for (int i = 0; i < entryCount; i++) {
            int key = keys[i];
            int off = offsets[i];
            writer.println("-- [" + i + "] storage:" + storageId + " key:" + key + " off:" + off);
            s.setPos(off);
            Value[] data = createRecord(writer, s);
            if (data != null) {
                createTemporaryTable(writer);
                writeRow(writer, s, data);
                if (remove && storageId == 0) {
                    String sql = data[3].getString();
                    if (sql.startsWith("CREATE USER ")) {
                        int saltIndex = ByteUtils.indexOf(s.getBytes(), "SALT ".getBytes(), off);
                        if (saltIndex >= 0) {
                            String userName = sql.substring("CREATE USER ".length(), sql.indexOf("SALT ") - 1);
                            if (userName.startsWith("\"")) {
                                // TODO doesn't work for all cases ("" inside user name)
                                userName = userName.substring(1, userName.length() - 1);
                            }
                            SHA256 sha = new SHA256();
                            byte[] userPasswordHash = sha.getKeyPasswordHash(userName, "".toCharArray());
                            byte[] salt = RandomUtils.getSecureBytes(Constants.SALT_LEN);
                            byte[] passwordHash = sha.getHashWithSalt(userPasswordHash, salt);
                            StringBuffer buff = new StringBuffer();
                            buff.append("SALT '");
                            buff.append(ByteUtils.convertBytesToString(salt));
                            buff.append("' HASH '");
                            buff.append(ByteUtils.convertBytesToString(passwordHash));
                            buff.append("'");
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

    private Value[] createRecord(PrintWriter writer, DataPage s) {
        recordLength = s.readInt();
        if (recordLength <= 0) {
            writeDataError(writer, "recordLength<0", s.getBytes(), blockCount);
            return null;
        }
        Value[] data;
        try {
            data = new Value[recordLength];
        } catch (OutOfMemoryError e) {
            writeDataError(writer, "out of memory", s.getBytes(), blockCount);
            return null;
        }
        return data;
    }

    private void writeRow(PrintWriter writer, DataPage s, Value[] data) {
        StringBuffer sb = new StringBuffer();
        sb.append("INSERT INTO O_" + storageId + " VALUES(");
        for (valueId = 0; valueId < recordLength; valueId++) {
            try {
                Value v = s.readValue();
                data[valueId] = v;
                if (valueId > 0) {
                    sb.append(", ");
                }
                sb.append(getSQL(v));
            } catch (Exception e) {
                writeDataError(writer, "exception " + e, s.getBytes(), blockCount);
                continue;
            } catch (OutOfMemoryError e) {
                writeDataError(writer, "out of memory", s.getBytes(), blockCount);
                continue;
            }
        }
        sb.append(");");
        writer.println(sb.toString());
        writer.flush();
        if (storageId == 0) {
            try {
                SimpleRow r = new SimpleRow(data);
                MetaRecord meta = new MetaRecord(r);
                schema.add(meta);
                if (meta.getObjectType() == DbObject.TABLE_OR_VIEW) {
                    String sql = data[3].getString();
                    String name = extractTableOrViewName(sql);
                    tableMap.put(ObjectUtils.getInteger(meta.getId()), name);
                }
            } catch (Throwable t) {
                writeError(writer, t);
            }
        }
    }

    private void dumpData(String fileName) {
        setDatabaseName(fileName.substring(0, fileName.length() - Constants.SUFFIX_DATA_FILE.length()));
        dumpData(fileName, fileName, FileStore.HEADER_LENGTH);
    }

    private void resetSchema() {
        schema = new ObjectArray();
        objectIdSet = new HashSet();
        tableMap = new HashMap();
    }

    private void dumpData(String fileName, String outputName, int offset) {
        PrintWriter writer = null;
        FileStore store = null;
        try {
            writer = getWriter(outputName, ".sql");
            writer.println("CREATE ALIAS IF NOT EXISTS READ_CLOB FOR \"" + this.getClass().getName() + ".readClob\";");
            writer.println("CREATE ALIAS IF NOT EXISTS READ_BLOB FOR \"" + this.getClass().getName() + ".readBlob\";");
            resetSchema();
            store = FileStore.open(null, fileName, "r");
            long length = store.length();
            int blockSize = DiskFile.BLOCK_SIZE;
            int blocks = (int) (length / blockSize);
            blockCount = 1;
            int[] pageOwners = new int[blocks / DiskFile.BLOCKS_PER_PAGE];
            for (block = 0; block < blocks; block += blockCount) {
                store.seek(offset + (long) block * blockSize);
                byte[] buff = new byte[blockSize];
                DataPage s = DataPage.create(this, buff);
                store.readFully(buff, 0, blockSize);
                blockCount = s.readInt();
                storageId = -1;
                recordLength = -1;
                valueId = -1;
                if (blockCount == 0) {
                    // free block
                    blockCount = 1;
                    continue;
                } else if (blockCount < 0) {
                    writeDataError(writer, "blockCount<0", s.getBytes(), 1);
                    blockCount = 1;
                    continue;
                } else if (((long) blockCount * blockSize) >= Integer.MAX_VALUE / 4 || (blockCount * blockSize) < 0) {
                    writeDataError(writer, "blockCount=" + blockCount, s.getBytes(), 1);
                    blockCount = 1;
                    continue;
                }
                writer.println("-- block " + block + " - " + (block + blockCount - 1));
                try {
                    s.checkCapacity(blockCount * blockSize);
                } catch (OutOfMemoryError e) {
                    writeDataError(writer, "out of memory", s.getBytes(), 1);
                    blockCount = 1;
                    continue;
                }
                if (blockCount > 1) {
                    if ((blockCount * blockSize) < 0) {
                        writeDataError(writer, "wrong blockCount", s.getBytes(), 1);
                        blockCount = 1;
                        continue;
                    }
                    try {
                        store.readFully(s.getBytes(), blockSize, blockCount * blockSize - blockSize);
                    } catch (Throwable e) {
                        writeDataError(writer, "eof", s.getBytes(), 1);
                        blockCount = 1;
                        store = FileStore.open(null, fileName, "r");
                        continue;
                    }
                }
                try {
                    s.check(blockCount * blockSize);
                } catch (SQLException e) {
                    writeDataError(writer, "wrong checksum", s.getBytes(), 1);
                    blockCount = 1;
                    continue;
                }
                storageId = s.readInt();
                if (storageId < 0) {
                    writeDataError(writer, "storageId<0", s.getBytes(), blockCount);
                    continue;
                }
                int page = block / DiskFile.BLOCKS_PER_PAGE;
                if (pageOwners[page] != 0 && pageOwners[page] != storageId) {
                    writeDataError(writer, "double allocation, previous=" + pageOwners[page] + " now=" + storageId, s
                            .getBytes(), blockCount);
                } else {
                    pageOwners[page] = storageId;
                }
                Value[] data = createRecord(writer, s);
                if (data != null) {
                    createTemporaryTable(writer);
                    writeRow(writer, s, data);
                }
            }
            writeSchema(writer);
            writer.close();
        } catch (Throwable e) {
            writeError(writer, e);
        } finally {
            IOUtils.closeSilently(writer);
            closeSilently(store);
        }
    }

    private void writeSchema(PrintWriter writer) {
        MetaRecord.sort(schema);
        for (int i = 0; i < schema.size(); i++) {
            MetaRecord m = (MetaRecord) schema.get(i);
            writer.println(m.getSQL() + ";");
        }
        for (Iterator it = tableMap.entrySet().iterator(); it.hasNext();) {
            Map.Entry entry = (Entry) it.next();
            Integer objectId = (Integer) entry.getKey();
            String name = (String) entry.getValue();
            if (objectIdSet.contains(objectId)) {
                writer.println("INSERT INTO " + name + " SELECT * FROM O_" + objectId + ";");
            }
        }
        for (Iterator it = objectIdSet.iterator(); it.hasNext();) {
            Integer objectId = (Integer) it.next();
            writer.println("DROP TABLE O_" + objectId + ";");
        }
        writer.println("DROP ALIAS READ_CLOB;");
        writer.println("DROP ALIAS READ_BLOB;");
    }

    private void createTemporaryTable(PrintWriter writer) {
        if (!objectIdSet.contains(ObjectUtils.getInteger(storageId))) {
            objectIdSet.add(ObjectUtils.getInteger(storageId));
            StringBuffer sb = new StringBuffer();
            sb.append("CREATE TABLE O_" + storageId + "(");
            for (int i = 0; i < recordLength; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append("C");
                sb.append(i);
                sb.append(" VARCHAR");
            }
            sb.append(");");
            writer.println(sb.toString());
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


    private void closeSilently(FileStore store) {
        if (store != null) {
            store.closeSilently();
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
    public FileStore openFile(String name, String mode, boolean mustExist) throws SQLException {
        return FileStore.open(this, name, "rw");
    }

    /**
     * INTERNAL
     */
    public int getChecksum(byte[] data, int start, int end) {
        int x = 0;
        while (start < end) {
            x += data[start++];
        }
        return x;
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
    public void handleInvalidChecksum() throws SQLException {
        throw new SQLException("Invalid Checksum");
    }

    /**
     * INTERNAL
     */
    public int compareTypeSave(Value a, Value b) {
        throw Message.throwInternalError();
    }

    /**
     * INTERNAL
     */
    public int getMaxLengthInplaceLob() {
        throw Message.throwInternalError();
    }

    /**
     * INTERNAL
     */
    public int allocateObjectId(boolean b, boolean c) {
        throw Message.throwInternalError();
    }

    /**
     * INTERNAL
     */
    public String createTempFile() {
        throw Message.throwInternalError();
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
    public boolean getLobFilesInDirectories() {
        return lobFilesInDirectories;
    }

    /**
     * INTERNAL
     */
    public SmallLRUCache getLobFileListCache() {
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
    public Trace getTrace() {
        return null;
    }

}
