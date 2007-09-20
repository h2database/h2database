/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.message.TraceSystem;


/**
 * @author Thomas
 */

public class FileUtils {

    public static final String MEMORY_PREFIX = "memFS:", MEMORY_PREFIX_LZF = "memLZF:";
    private static final HashMap MEMORY_FILES = new HashMap();
    // TODO detection of 'case in sensitive filesystem' could maybe implemented using some other means
    private static final boolean IS_FILE_SYSTEM_CASE_INSENSITIVE = (File.separatorChar == '\\');

    public static RandomAccessFile openRandomAccessFile(String fileName, String mode) throws IOException {
        fileName = translateFileName(fileName);
        try {
            RandomAccessFile file = new RandomAccessFile(fileName, mode);
            trace("openRandomAccessFile", fileName, file);
            return file;
        } catch (IOException e) {
            freeMemoryAndFinalize();
            return new RandomAccessFile(fileName, mode);
        }
    }

    public static void setLength(RandomAccessFile file, long newLength) throws IOException {
        try {
            trace("setLength", null, file);
            file.setLength(newLength);
        } catch (IOException e) {
            long length = file.length();
            if (newLength < length) {
                throw e;
            } else {
                long pos = file.getFilePointer();
                file.seek(length);
                long remaining = newLength - length;
                int maxSize = 1024 * 1024;
                int block = (int) Math.min(remaining, maxSize);
                byte[] buffer = new byte[block];
                while (remaining > 0) {
                    int write = (int) Math.min(remaining, maxSize);
                    file.write(buffer, 0, write);
                    remaining -= write;
                }
                file.seek(pos);
            }
        }
    }

    public static FileWriter openFileWriter(String fileName, boolean append) throws IOException {
        fileName = translateFileName(fileName);
        try {
            return new FileWriter(fileName, append);
        } catch (IOException e) {
            freeMemoryAndFinalize();
            return new FileWriter(fileName, append);
        }
    }

    public static boolean fileStartsWith(String fileName, String prefix) {
        fileName = translateFileName(fileName);
        if (IS_FILE_SYSTEM_CASE_INSENSITIVE) {
            fileName = StringUtils.toUpperEnglish(fileName);
            prefix = StringUtils.toUpperEnglish(prefix);
        }
        return fileName.startsWith(prefix);
    }

    public static FileInputStream openFileInputStream(String fileName) throws IOException {
        fileName = translateFileName(fileName);
        FileInputStream in = new FileInputStream(fileName);
        trace("openFileInputStream", fileName, in);
        return in;
    }

    public static FileOutputStream openFileOutputStream(String fileName) throws IOException, SQLException {
        fileName = translateFileName(fileName);
        try {
            File file = new File(fileName);
            FileUtils.createDirs(file.getAbsolutePath());
            FileOutputStream out = new FileOutputStream(fileName);
            trace("openFileOutputStream", fileName, out);
            return out;            
        } catch (IOException e) {
            freeMemoryAndFinalize();
            return new FileOutputStream(fileName);
        }
    }

    private static void freeMemoryAndFinalize() {
        trace("freeMemoryAndFinalize", null, null);
        Runtime rt = Runtime.getRuntime();
        long mem = rt.freeMemory();
        for (int i = 0; i < 16; i++) {
            rt.gc();
            long now = rt.freeMemory();
            rt.runFinalization();
            if (now == mem) {
                break;
            }
            mem = now;
        }
    }

    public static void rename(String oldName, String newName) throws SQLException {
        oldName = translateFileName(oldName);
        newName = translateFileName(newName);
        if (isInMemory(oldName)) {
            MemoryFile f = getMemoryFile(oldName);
            f.setName(newName);
            synchronized (MEMORY_FILES) {
                MEMORY_FILES.put(newName, f);
            }
            return;
        }
        File oldFile = new File(oldName);
        File newFile = new File(newName);
        if (oldFile.getName().equals(newFile.getName())) {
            throw Message.getInternalError("rename file old=new");
        }
        if (!oldFile.exists()) {
            throw Message.getSQLException(ErrorCode.FILE_RENAME_FAILED_2, new String[] { oldName + " (not found)",
                    newName });
        }
        if (newFile.exists()) {
            throw Message.getSQLException(ErrorCode.FILE_RENAME_FAILED_2,
                    new String[] { oldName, newName + " (exists)" });
        }
        for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
            trace("rename", oldName + " >" + newName, null);
            boolean ok = oldFile.renameTo(newFile);
            if (ok) {
                return;
            }
            wait(i);
        }
        throw Message.getSQLException(ErrorCode.FILE_RENAME_FAILED_2, new String[]{oldName, newName});
    }

    public static synchronized Properties loadProperties(String fileName) throws IOException {
        fileName = translateFileName(fileName);
        Properties prop = new SortedProperties();
        File file = new File(fileName);
        if (file.exists()) {
            FileInputStream in = new FileInputStream(file);
            try {
                prop.load(in);
            } finally {
                in.close();
            }
        }
        return prop;
    }

    public static boolean getBooleanProperty(Properties prop, String key, boolean def) {
        String value = prop.getProperty(key, ""+def);
        try {
            return Boolean.valueOf(value).booleanValue();
        } catch (Exception e) {
            TraceSystem.traceThrowable(e);
            return def;
        }
    }

    public static int getIntProperty(Properties prop, String key, int def) {
        String value = prop.getProperty(key, ""+def);
        try {
            return MathUtils.decodeInt(value);
        } catch (Exception e) {
            TraceSystem.traceThrowable(e);
            return def;
        }
    }

    public static void createDirs(String fileName) throws SQLException {
        fileName = translateFileName(fileName);
        File f = new File(fileName);
        if (!f.exists()) {
            String parent = f.getParent();
            if (parent == null) {
                return;
            }
            File dir = new File(parent);
            for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
                if (dir.exists() || dir.mkdirs()) {
                    return;
                }
                wait(i);
            }
            throw Message.getSQLException(ErrorCode.FILE_CREATION_FAILED_1, parent);
        }
    }

    public static boolean createNewFile(String fileName) throws SQLException {
        fileName = translateFileName(fileName);
        if (isInMemory(fileName)) {
            if (exists(fileName)) {
                return false;
            }
            // creates the file (not thread safe)
            getMemoryFile(fileName);
            return true;
        }
        File file = new File(fileName);
        for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
            try {
                return file.createNewFile();
            } catch (IOException e) {
                // TODO file lock: check if 'access denied' exceptions are
                // really a concurrent access problem
                wait(i);
            }
        }
        return false;
    }

    public static void delete(String fileName) throws SQLException {
        fileName = translateFileName(fileName);
        if (isInMemory(fileName)) {
            synchronized (MEMORY_FILES) {
                MEMORY_FILES.remove(fileName);
            }
            return;
        }
        File file = new File(fileName);
        if (file.exists()) {
            for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
                trace("delete", fileName, null);
                boolean ok = file.delete();
                if (ok) {
                    return;
                }
                wait(i);
            }
            throw Message.getSQLException(ErrorCode.FILE_DELETE_FAILED_1, fileName);
        }
    }

    private static void wait(int i) {
        if (i > 8) {
            System.gc();
        }
        try {
            // sleep at most 256 ms
            long sleep = Math.min(256, i * i);
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public static String getFileName(String name) throws SQLException {
        name = translateFileName(name);
        String separator = System.getProperty("file.separator");
        String path = getParent(name);
        if (!path.endsWith(separator)) {
            path += separator;
        }
        String fullFileName = normalize(name);
        if (!fullFileName.startsWith(path)) {
            throw Message.getInternalError("file utils error: " + fullFileName+" does not start with "+path);
        }
        String fileName = fullFileName.substring(path.length());
        return fileName;
    }

    public static String getFileInUserHome(String fileName) {
        String userDir = System.getProperty("user.home");
        if (userDir == null) {
            return fileName;
        }
        File file = new File(userDir, fileName);
        return file.getAbsolutePath();
    }

    public static String normalize(String fileName) throws SQLException {
        fileName = translateFileName(fileName);
        if (isInMemory(fileName)) {
            return fileName;
        }
        File f = new File(fileName);
        try {
            return f.getCanonicalPath();
        } catch (IOException e) {
            throw Message.convertIOException(e, fileName);
        }
    }

    public static void tryDelete(String fileName) {
        fileName = translateFileName(fileName);
        if (isInMemory(fileName)) {
            synchronized (MEMORY_FILES) {
                MEMORY_FILES.remove(fileName);
            }
            return;
        }
        trace("tryDelete", fileName, null);
        new File(fileName).delete();
    }

    public static boolean isReadOnly(String fileName) {
        fileName = translateFileName(fileName);
        if (isInMemory(fileName)) {
            return false;
        }
        File f = new File(fileName);
        return f.exists() && !f.canWrite();
    }

    public static boolean exists(String fileName) {
        fileName = translateFileName(fileName);
        if (isInMemory(fileName)) {
            synchronized (MEMORY_FILES) {
                return MEMORY_FILES.get(fileName) != null;
            }
        }
        return new File(fileName).exists();
    }

    public static MemoryFile getMemoryFile(String fileName) {
        synchronized (MEMORY_FILES) {
            MemoryFile m = (MemoryFile) MEMORY_FILES.get(fileName);
            if (m == null) {
                boolean compress = fileName.startsWith(MEMORY_PREFIX_LZF);
                m = new MemoryFile(fileName, compress);
                MEMORY_FILES.put(fileName, m);
            }
            return m;
        }
    }

    public static long length(String fileName) {
        fileName = translateFileName(fileName);
        if (isInMemory(fileName)) {
            return getMemoryFile(fileName).length();
        }
        return new File(fileName).length();
    }

    public static boolean isInMemory(String fileName) {
        return fileName.startsWith(MEMORY_PREFIX) || fileName.startsWith(MEMORY_PREFIX_LZF);
    }

    public static String createTempFile(String name, String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException, SQLException {
        name = translateFileName(name);
        name += ".";
        if (isInMemory(name)) {
            for (int i = 0;; i++) {
                String n = name + i + suffix;
                if (!exists(n)) {
                    // creates the file (not thread safe)
                    getMemoryFile(n);
                    return n;
                }
            }
        }
        String prefix = new File(name).getName();
        File dir;
        if (inTempDir) {
            dir = null;
        } else {
            dir = new File(name).getAbsoluteFile().getParentFile();
            dir.mkdirs();
        }
        File f = File.createTempFile(prefix, suffix, dir);
        if (deleteOnExit) {
            f.deleteOnExit();
        }
        // return f.getPath();
        return f.getCanonicalPath();
    }

    public static String getParent(String fileName) {
        fileName = translateFileName(fileName);
        if (isInMemory(fileName)) {
            return MEMORY_PREFIX;
        }
        return new File(fileName).getParent();
    }

    public static String[] listFiles(String path) throws SQLException {
        path = translateFileName(path);
        if (isInMemory(path)) {
            synchronized (MEMORY_FILES) {
                String[] list = new String[MEMORY_FILES.size()];
                MemoryFile[] l = new MemoryFile[MEMORY_FILES.size()];
                MEMORY_FILES.values().toArray(l);
                for (int i = 0; i < list.length; i++) {
                    list[i] = l[i].getName();
                }
                return list;
            }
        }
        File f = new File(path);
        try {
            String[] list = f.list();
            if (list == null) {
                return new String[0];
            }
            String base = f.getCanonicalPath();
            if (!base.endsWith(File.separator)) {
                base += File.separator;
            }
            for (int i = 0; i < list.length; i++) {
                list[i] = base + list[i];
            }
            return list;
        } catch (IOException e) {
            throw Message.convertIOException(e, path);
        }
    }

    public static boolean isDirectory(String fileName) {
        fileName = translateFileName(fileName);
        if (isInMemory(fileName)) {
            // TODO in memory file system currently doesn't support directories
            return false;
        }
        return new File(fileName).isDirectory();
    }

    public static void copy(String original, String copy) throws SQLException {
        original = translateFileName(original);
        copy = translateFileName(copy);
        FileOutputStream out = null;
        FileInputStream in = null;
        try {
            out = openFileOutputStream(copy);
            in = openFileInputStream(original);
            byte[] buffer = new byte[Constants.IO_BUFFER_SIZE];
            while (true) {
                int len = in.read(buffer);
                if (len < 0) {
                    break;
                }
                out.write(buffer, 0, len);
            }
        } catch (IOException e) {
            throw Message.convertIOException(e, "original: " + original + " copy: " + copy);
        } finally {
            IOUtils.closeSilently(in);
            IOUtils.closeSilently(out);
        }
    }

    public static void deleteRecursive(String fileName) throws SQLException {
        fileName = translateFileName(fileName);        
        if (FileUtils.isDirectory(fileName)) {
            String[] list = FileUtils.listFiles(fileName);
            for (int i = 0; list != null && i < list.length; i++) {
                deleteRecursive(list[i]);
            }
        }
        FileUtils.delete(fileName);
    }

    public static String translateFileName(String fileName) {
        if (fileName != null && fileName.startsWith("~")) {
            String userDir = System.getProperty("user.home");
            fileName = userDir + fileName.substring(1);
        }
        return fileName;
    }

    public static boolean isAbsolute(String fileName) {
        fileName = translateFileName(fileName);        
        File file = new File(fileName);
        return file.isAbsolute();
    }

    public static String getAbsolutePath(String fileName) {
        fileName = translateFileName(fileName);        
        File parent = new File(fileName).getAbsoluteFile();
        return parent.getAbsolutePath();    
    }

    public static long getLastModified(String fileName) {
        fileName = translateFileName(fileName);     
        return new File(fileName).lastModified();
    }

    public static Reader openFileReader(String fileName) throws IOException {
        fileName = translateFileName(fileName);     
        return new FileReader(fileName);
    }

    public static boolean canWrite(String fileName) {
        fileName = translateFileName(fileName);     
        return new File(fileName).canWrite();
    }
    
    static void trace(String method, String fileName, Object o) {
        if (SysProperties.TRACE_IO) {
            System.out.println("FileUtils." + method + " " + fileName + " " + o);
        }
    }

}
