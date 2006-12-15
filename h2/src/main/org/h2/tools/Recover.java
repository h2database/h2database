/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.h2.command.Parser;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.MetaRecord;
import org.h2.message.Message;
import org.h2.result.SimpleRow;
import org.h2.security.SHA256;
import org.h2.store.DataHandler;
import org.h2.store.DataPage;
import org.h2.store.DiskFile;
import org.h2.store.FileStore;
import org.h2.store.FileStoreInputStream;
import org.h2.store.LogFile;
import org.h2.util.ByteUtils;
import org.h2.util.IOUtils;
import org.h2.util.ObjectArray;
import org.h2.util.RandomUtils;
import org.h2.value.Value;

/**
 * Dumps the contents of a database file to a human readable text file.
 * This text file can be used to recover most of the data.
 * This tool does not open the database and can be used even if the database files are corrupted.
 * A database can get corrupted if there is a bug in the database engine or file system software, 
 * or if an application writes into the database file that doesn't understand the the file format, 
 * or if there is a hardware problem.
 * 
 * @author Thomas
 *
 */
public class Recover implements DataHandler {
    
    private String databaseName;    
    private boolean textStorage;
    private int block;
    private int blockCount;
    private int storageId;
    private int recordLength;
    private int valueId;
    private boolean log;

    private void showUsage() {
        System.out.println("java "+getClass().getName()+" [-dir <dir>] [-db <database>]");
    }
    
    /**
     * The command line interface for this tool.
     * The options must be split into strings like this: "-db", "test",... 
     * The following options are supported:
     * <ul>
     * <li>-help or -? (print the list of options)
     * <li>-dir directory (the default is the current directory)
     * <li>-db databaseName (all databases if no name is specified)
     * <li>-log {true|false} (log additional messages)
     * </ul>
     * 
     * @param args the command line arguments
     * @throws SQLException
     */    
    public static void main(String[] args) throws SQLException {
        new Recover().run(args);
    }
    
    private void run(String[] args) throws SQLException {
        String dir = ".";
        String db = null;
        boolean removePassword = false;
        for(int i=0; args != null && i<args.length; i++) {
            if("-dir".equals(args[i])) {
                dir = args[++i];
            } else if("-db".equals(args[i])) {
                db = args[++i];
            } else if("-removePassword".equals(args[i])) {
                removePassword = true;
            } else if("-log".equals(args[i])) {
                log = Boolean.valueOf(args[++i]).booleanValue();
            } else {
                showUsage();
                return;
            }
        }
        if(removePassword) {
            removePassword(dir, db);
        } else {
            execute(dir, db);
        }
    }
    
    private void removePassword(String dir, String db) throws SQLException {
        ArrayList list = FileBase.getDatabaseFiles(dir, db, true);
        for(int i=0; i<list.size(); i++) {
            String fileName = (String) list.get(i);
            if(fileName.endsWith(Constants.SUFFIX_DATA_FILE)) {
                removePassword(fileName);
            }
        }
    }
    
    private void logError(String message, Throwable t) {
        System.out.println(message + ": " + t.toString());
        if(log) {
            t.printStackTrace();
        }
    }

    private void removePassword(String fileName) throws SQLException {
        databaseName = fileName.substring(fileName.length() - Constants.SUFFIX_DATA_FILE.length());
        textStorage = Database.isTextStorage(fileName, false);
        byte[] magic = Database.getMagic(textStorage);
        FileStore store = FileStore.open(null, fileName, magic);
        long length = store.length();
        int offset = FileStore.HEADER_LENGTH;
        int blockSize = DiskFile.BLOCK_SIZE;
        int blocks = (int)(length / blockSize);
        blockCount = 1;
        for(int block=0; block<blocks; block += blockCount) {
            store.seek(offset + (long)block*blockSize);
            byte[] bytes = new byte[blockSize];
            DataPage s = DataPage.create(this, bytes);
            long start = store.getFilePointer();
            store.readFully(bytes, 0, blockSize);
            blockCount = s.readInt();
            storageId = -1;
            recordLength = -1;
            valueId = -1;
            if(blockCount == 0) {
                // free block
                blockCount = 1;
                continue;
            } else if(blockCount < 0) {
                blockCount = 1;
                continue;
            }
            try {
                s.checkCapacity(blockCount * blockSize);
            } catch(OutOfMemoryError e) {
                blockCount = 1;
                continue;
            }
            if(blockCount > 1) {
                store.readFully(s.getBytes(), blockSize, blockCount * blockSize - blockSize);
            }
            try {
                s.check(blockCount * blockSize);
            } catch(SQLException e) {
                blockCount = 1;
                continue;
            }
            storageId = s.readInt();
            if(storageId != 0) {
                continue;
            }
            recordLength = s.readInt();
            if(recordLength <= 0) {
                continue;
            }
            Value[] data;
            try {
                data = new Value[recordLength];
            } catch(Throwable e) {
                continue;
            }
            for(valueId=0; valueId<recordLength; valueId++) {
                try {
                    data[valueId] = s.readValue();
                } catch(Throwable e) {
                    continue;
                }
            }
            if(storageId == 0) {
                try {
                    String sql = data[3].getString();
                    if(!sql.startsWith("CREATE USER ")) {
                        continue;
                    }
                    int idx = sql.indexOf("SALT");
                    if(idx < 0) {
                        continue;
                    }
                    String userName = sql.substring("CREATE USER ".length(), idx-1);
                    if(userName.startsWith("\"")) {
                        // TODO doesn't work for all cases ("" inside user name)
                        userName = userName.substring(1, userName.length()-1);
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
                    if(admin) {
                        buff.append(" ADMIN");
                    }
                    byte[] replacement = buff.toString().getBytes();
                    int at = ByteUtils.indexOf(s.getBytes(), "CREATE USER ".getBytes(), 0);
                    System.arraycopy(replacement, 0, s.getBytes(), at, replacement.length);
                    s.fill(blockCount * blockSize);
                    s.updateChecksum();
                    store.seek(start);
                    store.write(s.getBytes(), 0, s.length());
                    System.out.println("User: " + userName);
                    break;
                } catch(Throwable e) {
                    e.printStackTrace();
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
        ArrayList list = FileBase.getDatabaseFiles(dir, db, true);
        for(int i=0; i<list.size(); i++) {
            String fileName = (String) list.get(i);
            // TODO recover: should create a working SQL script if possible (2 passes)
            if(fileName.endsWith(Constants.SUFFIX_DATA_FILE)) {
                dumpData(fileName);
            } else if(fileName.endsWith(Constants.SUFFIX_INDEX_FILE)) {
                dumpIndex(fileName);
            } else if(fileName.endsWith(Constants.SUFFIX_LOG_FILE)) {
                dumpLog(fileName);
            } else if(fileName.endsWith(Constants.SUFFIX_LOB_FILE)) {
                dumpLob(fileName);
            }
        }
    }
    
    private static PrintWriter getWriter(String fileName, String suffix) throws IOException {
        fileName = fileName.substring(0, fileName.length()-3);
        String outputFile = fileName + suffix;
        System.out.println("Created file: " + outputFile);
        return new PrintWriter(new BufferedWriter(new FileWriter(outputFile)));
    }
    
    private void writeDataError(PrintWriter writer, String error, byte[] data, int dumpBlocks) throws IOException {
        writer.println("-- ERROR:" + error + " block:"+block+" blockCount:"+blockCount+" storageId:" + storageId+" recordLength: " + recordLength+" valudId:" + valueId);
        StringBuffer sb = new StringBuffer();
        for(int i=0; i<dumpBlocks * DiskFile.BLOCK_SIZE; i++) {
            int x = (data[i] & 0xff);
            if(x >= ' ' && x < 128) {
                sb.append((char)x);
            } else {
                sb.append('?');
            }
        }
        for(int i=0; i<dumpBlocks * DiskFile.BLOCK_SIZE; i++) {
            int x = (data[i] & 0xff);
            sb.append(' ');
            if(x < 16) {
                sb.append('0');
            }
            sb.append(Integer.toHexString(x));
        }
        writer.println("-- dump: " + sb.toString());
    }
    
    private void dumpLob(String fileName) {
        FileOutputStream out = null;
        FileStore store = null;
        try {
            out = new FileOutputStream(fileName + ".txt");
            textStorage = Database.isTextStorage(fileName, false);
            byte[] magic = Database.getMagic(textStorage);
            store = FileStore.open(null, fileName, magic);
            store.init();
            boolean compression = true;
            InputStream in = new BufferedInputStream(new FileStoreInputStream(store, this, compression));
            byte[] buffer = new byte[Constants.IO_BUFFER_SIZE];
            while(true) {
                int l = in.read(buffer);
                if(l<0) {
                    break;
                }
                out.write(buffer, 0, l);
            }
        } catch(Throwable e) {
            logError(fileName, e);
        } finally {
            IOUtils.closeSilently(out);
            closeSilently(store);
        }
    }
    
    private void dumpLog(String fileName) throws SQLException {
        PrintWriter writer = null;        
        FileStore store = null;
        try {
            databaseName = fileName.substring(fileName.length() - Constants.SUFFIX_LOG_FILE.length());
            writer = getWriter(fileName, ".txt");
            textStorage = Database.isTextStorage(fileName, false);
            byte[] magic = Database.getMagic(textStorage);
            store = FileStore.open(null, fileName, magic);
            long length = store.length();
            writer.println("// length: " + length);
            int offset = FileStore.HEADER_LENGTH;
            int blockSize = LogFile.BLOCK_SIZE;
            int blocks = (int)(length / blockSize);
            byte[] buff = new byte[blockSize];
            DataPage s = DataPage.create(this, buff);
            s.fill(3*blockSize);
            int len = s.length();
            s.reset();
            if(length < FileStore.HEADER_LENGTH + len) {
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
            int max = (int)(length / blockSize);
            writer.println("// max:" + max);
            while(true) {
                int pos = (int)(store.getFilePointer() / blockSize);
                if((long)pos * blockSize >= length) {
                    break;
                }
                buff = new byte[blockSize];
                store.readFully(buff, 0, blockSize);
                s = DataPage.create(this, buff);
                blocks = Math.abs(s.readInt());
                if(blocks > 1) {
                    byte[] b2 = new byte[blocks * blockSize];
                    System.arraycopy(buff, 0, b2, 0, blockSize);
                    buff = b2;
                    store.readFully(buff, blockSize, blocks * blockSize - blockSize);
                    s = DataPage.create(this, buff);
                    s.check(blocks * blockSize);
                } else {
                    s.reset();
                }
                blocks = s.readInt();
                if(blocks<=0) {
                    writer.println("// [" + pos+"] blocks: "+blocks+" (end)");
                    break;
                } else {
                    char type = (char)s.readByte();
                    int sessionId = s.readInt();
                    if(type == 'P') {
                        String transaction = s.readString();
                        writer.println("//   prepared session:"+sessionId+" tx: " + transaction);
                    } else if(type == 'C') {
                        writer.println("//   commit session:" + sessionId);
                    } else {
                        int storageId = s.readInt();
                        int recordId = s.readInt();
                        int blockCount = s.readInt();
                        if(type != 'T') {
                            s.readDataPageNoSize();
                        }
                        switch(type) {
                        case 'S': {
                            char fileType = (char) s.readByte();
                            int sumLength= s.readInt();
                            byte[] summary = new byte[sumLength];
                            if(sumLength > 0) {
                                s.read(summary, 0, sumLength);
                            }
                            writer.println("//   summary session:"+sessionId+" fileType: " + fileType + " sumLength: " + sumLength);
                            dumpSummary(writer, summary);
                            break;
                        }
                        case 'T':
                            writer.println("//   truncate session:"+sessionId+" storage: " + storageId + " recordId: " + recordId + " blockCount: "+blockCount);
                            break;
                        case 'I':
                            writer.println("//   insert session:"+sessionId+" storage: " + storageId + " recordId: " + recordId + " blockCount: "+blockCount);
                            break;
                        case 'D':
                            writer.println("//   delete session:"+sessionId+" storage: " + storageId + " recordId: " + recordId + " blockCount: "+blockCount);
                            break;
                        default:
                            writer.println("//   type?:"+type+" session:"+sessionId+" storage: " + storageId + " recordId: " + recordId + " blockCount: "+blockCount);
                            break;
                        }
                    }                    
                }
            }
        } catch(Throwable e) {
            writeError(writer, e);
        } finally {
            IOUtils.closeSilently(writer);
            closeSilently(store);
        }
    }
    
    private void dumpSummary(PrintWriter writer, byte[] summary) throws SQLException {
        if(summary == null || summary.length==0) {
            writer.println("//     summary is empty");
            return;
        }
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(summary));
            int b2 = in.readInt();
            for(int i=0; i<b2 / 8; i++) {
                in.read();
            }
            int len = in.readInt();
            for(int i=0; i<len; i++) {
                int storageId = in.readInt();
                if(storageId != -1) {
                    writer.println("//     pos:"+(i*DiskFile.BLOCKS_PER_PAGE)+" storage:" + storageId);
                }
            }
            while(true) {
                int s = in.readInt();
                if(s < 0) {
                    break;
                }
                int recordCount = in.readInt();
                writer.println("//     storage:"+s+" recordCount:" + recordCount);
            }
        } catch(Throwable e) {
            writeError(writer, e);
        }
    }

    private void dumpIndex(String fileName) throws SQLException {
        PrintWriter writer = null;        
        FileStore store = null;
        try {
            databaseName = fileName.substring(fileName.length() - Constants.SUFFIX_INDEX_FILE.length());
            writer = getWriter(fileName, ".txt");
            textStorage = Database.isTextStorage(fileName, false);
            byte[] magic = Database.getMagic(textStorage);
            store = FileStore.open(null, fileName, magic);
            long length = store.length();
            int offset = FileStore.HEADER_LENGTH;
            int blockSize = DiskFile.BLOCK_SIZE;
            int blocks = (int)(length / blockSize);
            blockCount = 1;
            int[] pageOwners = new int[blocks / DiskFile.BLOCKS_PER_PAGE];
            for(int block=0; block<blocks; block += blockCount) {
                store.seek(offset + (long)block*blockSize);
                byte[] buff = new byte[blockSize];
                DataPage s = DataPage.create(this, buff);
                store.readFully(buff, 0, blockSize);
                blockCount = s.readInt();
                storageId = -1;
                recordLength = -1;
                valueId = -1;
                if(blockCount == 0) {
                    // free block
                    blockCount = 1;
                    continue;
                } else if(blockCount < 0) {
                    writeDataError(writer, "blockCount<0", s.getBytes(), 1);
                    blockCount = 1;
                    continue;
                } else if((blockCount * blockSize) >= Integer.MAX_VALUE / 4) {
                    writeDataError(writer, "blockCount=" + blockCount, s.getBytes(), 1);
                    blockCount = 1;
                    continue;
                }
                try {
                    s.checkCapacity(blockCount * blockSize);
                } catch(OutOfMemoryError e) {
                    writeDataError(writer, "out of memory", s.getBytes(), 1);
                    blockCount = 1;
                    continue;
                }
                if(blockCount > 1) {
                    store.readFully(s.getBytes(), blockSize, blockCount * blockSize - blockSize);
                }
                try {
                    s.check(blockCount * blockSize);
                } catch(SQLException e) {
                    writeDataError(writer, "wrong checksum", s.getBytes(), 1);
                    blockCount = 1;
                    continue;
                }
                storageId = s.readInt();
                if(storageId < 0) {
                    writeDataError(writer, "storageId<0", s.getBytes(), blockCount);
                    continue;
                }
                int page = block / DiskFile.BLOCKS_PER_PAGE;
                if(pageOwners[page] != 0 && pageOwners[page] != storageId) {
                    writeDataError(writer, "double allocation, previous="+pageOwners[page]+" now="+storageId, s.getBytes(), blockCount);
                } else {
                    pageOwners[page] = storageId;
                }
                writer.println("// [" + block + "] page:"+page+" blocks:"+blockCount+" storage:"+storageId);
            }
        } catch(Throwable e) {
            writeError(writer, e);
        } finally {
            IOUtils.closeSilently(writer);
            closeSilently(store);
        }
    }

    private void dumpData(String fileName) throws SQLException {
        PrintWriter writer = null;        
        FileStore store = null;
        try {
            databaseName = fileName.substring(fileName.length() - Constants.SUFFIX_DATA_FILE.length());
            writer = getWriter(fileName, ".sql");
            ObjectArray schema = new ObjectArray();
            HashSet objectIdSet = new HashSet();
            HashMap tableMap = new HashMap();
            textStorage = Database.isTextStorage(fileName, false);
            byte[] magic = Database.getMagic(textStorage);
            store = FileStore.open(null, fileName, magic);
            long length = store.length();
            int offset = FileStore.HEADER_LENGTH;
            int blockSize = DiskFile.BLOCK_SIZE;
            int blocks = (int)(length / blockSize);
            blockCount = 1;
            int[] pageOwners = new int[blocks / DiskFile.BLOCKS_PER_PAGE];
            for(int block=0; block<blocks; block += blockCount) {
                store.seek(offset + (long)block*blockSize);
                byte[] buff = new byte[blockSize];
                DataPage s = DataPage.create(this, buff);
                store.readFully(buff, 0, blockSize);
                blockCount = s.readInt();
                storageId = -1;
                recordLength = -1;
                valueId = -1;
                if(blockCount == 0) {
                    // free block
                    blockCount = 1;
                    continue;
                } else if(blockCount < 0) {
                    writeDataError(writer, "blockCount<0", s.getBytes(), 1);
                    blockCount = 1;
                    continue;
                } else if((blockCount * blockSize) >= Integer.MAX_VALUE / 4) {
                    writeDataError(writer, "blockCount=" + blockCount, s.getBytes(), 1);
                    blockCount = 1;
                    continue;
                }
                try {
                    s.checkCapacity(blockCount * blockSize);
                } catch(OutOfMemoryError e) {
                    writeDataError(writer, "out of memory", s.getBytes(), 1);
                    blockCount = 1;
                    continue;
                }
                if(blockCount > 1) {
                    store.readFully(s.getBytes(), blockSize, blockCount * blockSize - blockSize);
                }
                try {
                    s.check(blockCount * blockSize);
                } catch(SQLException e) {
                    writeDataError(writer, "wrong checksum", s.getBytes(), 1);
                    blockCount = 1;
                    continue;
                }
                storageId = s.readInt();
                if(storageId < 0) {
                    writeDataError(writer, "storageId<0", s.getBytes(), blockCount);
                    continue;
                }
                int page = block / DiskFile.BLOCKS_PER_PAGE;
                if(pageOwners[page] != 0 && pageOwners[page] != storageId) {
                    writeDataError(writer, "double allocation, previous="+pageOwners[page]+" now="+storageId, s.getBytes(), blockCount);
                } else {
                    pageOwners[page] = storageId;
                }
                recordLength = s.readInt();
                if(recordLength <= 0) {
                    writeDataError(writer, "recordLength<0", s.getBytes(), blockCount);
                    continue;
                }
                Value[] data;
                try {
                    data = new Value[recordLength];
                } catch(OutOfMemoryError e) {
                    writeDataError(writer, "out of memory", s.getBytes(), blockCount);
                    continue;
                }
                if(!objectIdSet.contains(new Integer(storageId))) {
                    objectIdSet.add(new Integer(storageId));
                    StringBuffer sb = new StringBuffer();
                    sb.append("CREATE TABLE O_" + storageId + "(");
                    for(int i=0; i<recordLength; i++) {
                        if(i>0) {
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
                StringBuffer sb = new StringBuffer();
                sb.append("INSERT INTO O_" + storageId + " VALUES(");
                for(valueId=0; valueId<recordLength; valueId++) {
                    try {
                        data[valueId] = s.readValue();
                        if(valueId>0) {
                            sb.append(", ");
                        }
                        sb.append(data[valueId].getSQL());
                    } catch(Exception e) {
                        writeDataError(writer, "exception " + e, s.getBytes(), blockCount);
                        continue;
                    } catch(OutOfMemoryError e) {
                        writeDataError(writer, "out of memory", s.getBytes(), blockCount);
                        continue;
                    }
                }
                sb.append(");");
                writer.println(sb.toString());
                writer.flush();
                if(storageId == 0) {
                    try {
                        SimpleRow r = new SimpleRow(data);
                        MetaRecord meta = new MetaRecord(r);
                        schema.add(meta);
                        if(meta.getObjectType() == DbObject.TABLE_OR_VIEW) {
                            String sql = data[3].getString();
                            int end = sql.indexOf('(');
                            if(end >= 0) {
                                int start = sql.lastIndexOf(' ', end);
                                String name = sql.substring(start, end).trim();
                                tableMap.put(new Integer(meta.getId()), name);
                            }
                        }
                    } catch(Throwable t) {
                        writeError(writer, t);
                    }
                }
            }
            MetaRecord.sort(schema);
            for(int i=0; i<schema.size(); i++) {
                MetaRecord m = (MetaRecord) schema.get(i);
                writer.println(m.getSQL() + ";");
            }
            for(Iterator it = tableMap.keySet().iterator(); it.hasNext(); ) {
                Integer objectId = (Integer) it.next();
                String name = (String) tableMap.get(objectId);
                writer.println("INSERT INTO " + name +" SELECT * FROM O_" + objectId + ";");
            }
            for(Iterator it = objectIdSet.iterator(); it.hasNext(); ) {
                Integer objectId = (Integer) it.next();
                writer.println("DROP TABLE O_" + objectId + ";");
            }
        } catch(Throwable e) {
            writeError(writer, e);
        } finally {
            IOUtils.closeSilently(writer);
            closeSilently(store);
        }
    }

    private void closeSilently(FileStore store) {
        if(store != null) {
            store.closeSilently();
            store = null;
        }
    }
    
    private void writeError(PrintWriter writer, Throwable e) {
        if(writer != null) {
            writer.println("// error: "+ e);
        }
        logError("Error", e);
    }

    /**
     * INTERNAL
     */
    public boolean getTextStorage() {
        return textStorage;
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
    public FileStore openFile(String name, boolean mustExist) throws SQLException {
        return null;
    }

    /**
     * INTERNAL
     */
    public int getChecksum(byte[] data, int start, int end) {
        int x = 0;
        while(start < end) {
            x += data[start++];
        }
        return x;
    }

    /**
     * INTERNAL
     */
    public void checkPowerOff() throws SQLException {
    }

    /**
     * INTERNAL
     */
    public void checkWritingAllowed() throws SQLException {
    }

    /**
     * INTERNAL
     */
    public void freeUpDiskSpace() throws SQLException {
    }

    /**
     * INTERNAL
     */
    public void handleInvalidChecksum() throws SQLException {
    }

    /**
     * INTERNAL
     */    
    public int compareTypeSave(Value a, Value b) throws SQLException {
        throw Message.getInternalError();
    }

    /**
     * INTERNAL
     */    
    public int getMaxLengthInplaceLob() {
        throw Message.getInternalError();
    }

    /**
     * INTERNAL
     */    
    public int allocateObjectId(boolean b, boolean c) {
        throw Message.getInternalError();
    }

    /**
     * INTERNAL
     */    
    public String createTempFile() throws SQLException {
        throw Message.getInternalError();
    }

    /**
     * INTERNAL
     */    
    public String getLobCompressionAlgorithm(int type) {
        return null;
    }

}
