/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;

import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.message.TraceSystem;


/**
 * @author Thomas
 */

public class FileUtils {

    public static HashMap memoryFiles = new HashMap();
    private static final String MEMORY_PREFIX = "inmemory:";
    private static final String MEMORY_PREFIX_2 = "mem:";
    // TODO detection of 'case in sensitive filesystem' could maybe implemented using some other means
    private static final boolean isCaseInsensitiveFileSystem = (File.separatorChar == '\\');

    // TODO gcj: use our own UTF-8 encoder

    public static RandomAccessFile openRandomAccessFile(String fileName, String mode) throws IOException {
        try {
            return new RandomAccessFile(fileName, mode);
        } catch(IOException e) {
            freeMemoryAndFinalize();
            return new RandomAccessFile(fileName, mode);
        }
    }

    public static void setLength(RandomAccessFile file, long newLength) throws IOException {
        try {
            file.setLength(newLength);
        } catch(IOException e) {
            long length = file.length();
            if(newLength < length) {
                throw e;
            } else {
                long pos = file.getFilePointer();
                file.seek(length);
                long remaining = newLength - length;
                int maxSize = 1024 * 1024;
                int block = (int)Math.min(remaining, maxSize);
                byte[] buffer = new byte[block];
                while(remaining > 0) {
                    int write = (int) Math.min(remaining, maxSize);
                    file.write(buffer, 0, write);
                    remaining -= write;
                }
                file.seek(pos);
            }
        }
    }

    public static FileWriter openFileWriter(String fileName, boolean append) throws IOException {
        try {
            return new FileWriter(fileName, append);
        } catch(IOException e) {
            freeMemoryAndFinalize();
            return new FileWriter(fileName, append);
        }
    }

    public static boolean fileStartsWith(String fileName, String prefix) {
        if(isCaseInsensitiveFileSystem) {
            fileName = StringUtils.toUpperEnglish(fileName);
            prefix = StringUtils.toUpperEnglish(prefix);
        }
        return fileName.startsWith(prefix);
    }

    public static FileOutputStream openFileOutputStream(File file) throws IOException {
        try {
            return new FileOutputStream(file);
        } catch(IOException e) {
            freeMemoryAndFinalize();
            return new FileOutputStream(file);
        }
    }

    private static void freeMemoryAndFinalize() {
        long mem = Runtime.getRuntime().freeMemory();
        for(int i=0; i<16; i++) {
            System.gc();
            long now = Runtime.getRuntime().freeMemory();
            Runtime.getRuntime().runFinalization();
            if(now == mem) {
                break;
            }
            mem = now;
        }
    }

    public static void rename(String oldName, String newName) throws SQLException {
        if(isInMemory(oldName)) {
            MemoryFile f = getMemoryFile(oldName);
            f.setName(newName);
            memoryFiles.put(newName, f);
            return;
        }
        File oldFile = new File(oldName);
        File newFile = new File(newName);
        if(oldFile.getName().equals(newFile.getName())) {
            throw Message.getInternalError("rename file old=new");
        }
        if(oldFile.exists() && !newFile.exists()) {
            for(int i=0; i<16; i++) {
                boolean ok = oldFile.renameTo(newFile);
                if(ok) {
                    return;
                }
                wait(i);
            }
        }
        throw Message.getSQLException(Message.FILE_RENAME_FAILED_2, new String[]{oldName, newName}, null);
    }

    public static synchronized Properties loadProperties(File file) throws IOException {
        Properties prop = new Properties();
        if(file.exists()) {
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
        } catch(Exception e) {
            TraceSystem.traceThrowable(e);
            return def;
        }
    }

    public static int getIntProperty(Properties prop, String key, int def) {
        String value = prop.getProperty(key, ""+def);
        try {
            return MathUtils.decodeInt(value);
        } catch(Exception e) {
            TraceSystem.traceThrowable(e);
            return def;
        }
    }

    public static void createDirs(String fileName) throws SQLException {
        File f = new File(fileName);
        if(!f.exists()) {
            String parent = f.getParent();
            if(parent == null) {
                return;
            }
            File dir = new File(parent);
            for(int i=0; i<16; i++) {
                if(dir.exists() || dir.mkdirs()) {
                    return;
                }
                wait(i);
            }
            throw Message.getSQLException(Message.FILE_CREATION_FAILED_1, parent);
        }
    }

    public static boolean createNewFile(String fileName) throws SQLException {
        if(isInMemory(fileName)) {
            if(exists(fileName)) {
                return false;
            }
            // creates the file (not thread safe)
            getMemoryFile(fileName);
            return true;
        }
        File file = new File(fileName);
        for(int i=0; i<8; i++) {
            try {
                return file.createNewFile();
            } catch (IOException e) {
                // TODO file lock: check if 'access denied' exceptions are really a concurrent access problem
                wait(i);
            }
        }
        // TODO GCJ: it seems gcj throws 'CreateFile failed' if the file already exists?!
        return false;
        // TODO is this message used elsewhere?
        // throw Message.getSQLException(Message.FILE_CREATION_FAILED_1, fileName);
    }

    public static void delete(String fileName) throws SQLException {
        if(isInMemory(fileName)) {
            memoryFiles.remove(fileName);
            return;
        }
        File file = new File(fileName);
        if(file.exists()) {
            for(int i=0; i<16; i++) {
                boolean ok = file.delete();
                if(ok) {
                    return;
                }
                wait(i);
            }
            throw Message.getSQLException(Message.FILE_DELETE_FAILED_1, fileName);
        }
    }

    private static void wait(int i) {
        if(i > 8) {
            System.gc();
        }
        try {
            // sleep at most 256 ms
            Thread.sleep(i * i);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public static String getFileName(String name) throws SQLException {
        String separator = System.getProperty("file.separator");
        String path = getParent(name) + separator;
        String fullFileName = normalize(name);
        if(!fullFileName.startsWith(path)) {
            throw Message.getInternalError("file utils error: " + fullFileName+" does not start with "+path);
        }
        String fileName = fullFileName.substring(path.length());
        return fileName;
    }

    public static File getFileInUserHome(String filename) {
        String userDir = System.getProperty("user.home");
        if(userDir == null) {
            return new File(filename);
        }
        return new File(userDir, filename);
    }

    public static String normalize(String fileName) throws SQLException {
        if(isInMemory(fileName)) {
            return fileName;
        }
        File f = new File(fileName);
        try {
            return f.getCanonicalPath();
        } catch (IOException e) {
            throw Message.convert(e);
        }
    }

    public static void tryDelete(String fileName) {
        if(isInMemory(fileName)) {
            memoryFiles.remove(fileName);
            return;
        }
        new File(fileName).delete();
    }

    public static boolean isReadOnly(String fileName) {
        if(isInMemory(fileName)) {
            return false;
        }
        File f = new File(fileName);
        return f.exists() && !f.canWrite();
    }

    public static boolean exists(String fileName) {
        if(isInMemory(fileName)) {
            return memoryFiles.get(fileName) != null;
        }
        return new File(fileName).exists();
    }

    public static MemoryFile getMemoryFile(String fileName) {
        MemoryFile m = (MemoryFile)memoryFiles.get(fileName);
        if(m == null) {
            m = new MemoryFile(fileName);
            memoryFiles.put(fileName, m);
        }
        return m;
    }

    public static long length(String fileName) {
        if(isInMemory(fileName)) {
            return getMemoryFile(fileName).length();
        }
        return new File(fileName).length();
    }

    public static boolean isInMemory(String fileName) {
        return fileName.startsWith(MEMORY_PREFIX) || fileName.startsWith(MEMORY_PREFIX_2);
    }

    public static String createTempFile(String name, String suffix, boolean deleteOnExit) throws IOException {
        name += ".";
        if(isInMemory(name)) {
            for(int i=0;; i++) {
                String n = name + i + suffix;
                if(!exists(n)) {
                    // creates the file (not thread safe)
                    getMemoryFile(n);
                    return n;
                }
            }
        }
        String prefix = new File(name).getName();
        File dir = new File(name).getAbsoluteFile().getParentFile();
        File f = File.createTempFile(prefix, suffix, dir);
        if(deleteOnExit) {
            f.deleteOnExit();
        }
        return f.getCanonicalPath();
    }

    public static String getParent(String fileName) {
        if(isInMemory(fileName)) {
            return MEMORY_PREFIX;
        }
        return new File(fileName).getParent();
    }

    public static String[] listFiles(String path) throws SQLException {
        if(isInMemory(path)) {
            String[] list = new String[memoryFiles.size()];
            MemoryFile[] l = new MemoryFile[memoryFiles.size()];
            memoryFiles.values().toArray(l);
            for(int i=0; i<list.length; i++) {
                list[i] = l[i].getName();
            }
            return list;
        }
        try {
            File[] files = new File(path).listFiles();
            if(files == null) {
                return new String[0];
            }
            String[] list = new String[files.length];
            for(int i=0; i<files.length; i++) {
                list[i] = files[i].getCanonicalPath();
            }
            return list;
        } catch (IOException e) {
            throw Message.convert(e);
        }
    }

    public static boolean isDirectory(String fileName) {
        if(isInMemory(fileName)) {
            // TODO inmemory: currently doesn't support directories
            return false;
        }
        return new File(fileName).isDirectory();
    }

    public static void copy(String original, String copy) throws SQLException {
        FileOutputStream out = null;
        FileInputStream in = null;
        try {
            out = openFileOutputStream(new File(copy));
            in = new FileInputStream(new File(original));
            byte[] buffer = new byte[Constants.IO_BUFFER_SIZE];
            while(true) {
                int len = in.read(buffer);
                if(len < 0) {
                    break;
                }
                out.write(buffer, 0, len);
            }
        } catch(IOException e) {
            IOUtils.closeSilently(in);
            IOUtils.closeSilently(out);
            throw Message.convert(e);
        }
    }

    public static void deleteRecursive(String fileName) throws SQLException {
        if(FileUtils.isDirectory(fileName)) {
            String[] list = FileUtils.listFiles(fileName);
            for(int i=0; list != null && i<list.length; i++) {
                deleteRecursive(list[i]);
            }
        }
        FileUtils.delete(fileName);
    }
    
}
