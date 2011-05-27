/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.util.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;

public class FileSystemDisk extends FileSystem {

    private static final FileSystemDisk INSTANCE = new FileSystemDisk();
    // TODO detection of 'case in sensitive filesystem' could maybe implemented using some other means
    private static final boolean IS_FILE_SYSTEM_CASE_INSENSITIVE = (File.separatorChar == '\\');

    public static FileSystemDisk getInstance() {
        return INSTANCE;
    }
    
    private FileSystemDisk() {
    }

    public long length(String fileName) {
        fileName = translateFileName(fileName);
        return new File(fileName).length();
    }
    
    private String translateFileName(String fileName) {
        if (fileName != null && fileName.startsWith("~")) {
            String userDir = System.getProperty("user.home");
            fileName = userDir + fileName.substring(1);
        }
        return fileName;
    }

    public void rename(String oldName, String newName) throws SQLException {
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
    
    void trace(String method, String fileName, Object o) {
        if (SysProperties.TRACE_IO) {
            System.out.println("FileSystem." + method + " " + fileName + " " + o);
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

    public boolean createNewFile(String fileName) throws SQLException {
        fileName = translateFileName(fileName);
        File file = new File(fileName);
        for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
            try {
                return file.createNewFile();
            } catch (IOException e) {
                // 'access denied' is really a concurrent access problem
                wait(i);
            }
        }
        return false;
    }

    public boolean exists(String fileName) {
        fileName = translateFileName(fileName);
        return new File(fileName).exists();
    }

    public void delete(String fileName) throws SQLException {
        fileName = translateFileName(fileName);
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

    public boolean tryDelete(String fileName) {
        fileName = translateFileName(fileName);
        trace("tryDelete", fileName, null);
        return new File(fileName).delete();
    }
    
    public String createTempFile(String name, String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException {
        name = translateFileName(name);
        name += ".";
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
        return f.getCanonicalPath();
    }
    
    public String[] listFiles(String path) throws SQLException {
        path = translateFileName(path);
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
    
    public void deleteRecursive(String fileName) throws SQLException {
        fileName = translateFileName(fileName);        
        if (FileUtils.isDirectory(fileName)) {
            String[] list = listFiles(fileName);
            for (int i = 0; list != null && i < list.length; i++) {
                deleteRecursive(list[i]);
            }
        }
        delete(fileName);
    }
    
    public boolean isReadOnly(String fileName) {
        fileName = translateFileName(fileName);
        File f = new File(fileName);
        return f.exists() && !f.canWrite();
    }

    public String normalize(String fileName) throws SQLException {
        fileName = translateFileName(fileName);
        File f = new File(fileName);
        try {
            return f.getCanonicalPath();
        } catch (IOException e) {
            throw Message.convertIOException(e, fileName);
        }
    }
    
    public String getParent(String fileName) {
        fileName = translateFileName(fileName);
        return new File(fileName).getParent();
    }
    
    public boolean isDirectory(String fileName) {
        fileName = translateFileName(fileName);
        return new File(fileName).isDirectory();
    }
    
    public boolean isAbsolute(String fileName) {
        fileName = translateFileName(fileName);        
        File file = new File(fileName);
        return file.isAbsolute();
    }
    
    public String getAbsolutePath(String fileName) {
        fileName = translateFileName(fileName);        
        File parent = new File(fileName).getAbsoluteFile();
        return parent.getAbsolutePath();    
    }

    public long getLastModified(String fileName) {
        fileName = translateFileName(fileName);     
        return new File(fileName).lastModified();
    }
    
    public boolean canWrite(String fileName) {
        fileName = translateFileName(fileName);     
        return new File(fileName).canWrite();
    }

    public void copy(String original, String copy) throws SQLException {
        original = translateFileName(original);
        copy = translateFileName(copy);
        OutputStream out = null;
        InputStream in = null;
        try {
            out = FileUtils.openFileOutputStream(copy, false);
            in = FileUtils.openFileInputStream(original);
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

    public void createDirs(String fileName) throws SQLException {
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
    
    public String getFileName(String name) throws SQLException {
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
    
    public boolean fileStartsWith(String fileName, String prefix) {
        fileName = translateFileName(fileName);
        if (IS_FILE_SYSTEM_CASE_INSENSITIVE) {
            fileName = StringUtils.toUpperEnglish(fileName);
            prefix = StringUtils.toUpperEnglish(prefix);
        }
        return fileName.startsWith(prefix);
    }
    
    public OutputStream openFileOutputStream(String fileName, boolean append) throws SQLException {
        fileName = translateFileName(fileName);
        try {
            File file = new File(fileName);
            createDirs(file.getAbsolutePath());
            FileOutputStream out = new FileOutputStream(fileName, append);
            trace("openFileOutputStream", fileName, out);
            return out;            
        } catch (IOException e) {
            freeMemoryAndFinalize();
            try {
                return new FileOutputStream(fileName);
            } catch (IOException e2) {
                throw Message.convertIOException(e, fileName);
            }
        }
    }
    
    public InputStream openFileInputStream(String fileName) throws IOException {
        if (fileName.indexOf(':') > 1) {
            // if the : is in position 1, a windows file access is assumed: C:.. or D:
            // otherwise a URL is assumed
            URL url = new URL(fileName);
            InputStream in = url.openStream();
            return in;
        }
        fileName = translateFileName(fileName);
        FileInputStream in = new FileInputStream(fileName);
        trace("openFileInputStream", fileName, in);
        return in;
    }
    
    private void freeMemoryAndFinalize() {
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

    public FileObject openFileObject(String fileName, String mode) throws IOException {
        fileName = translateFileName(fileName);
        RandomAccessFile f;
        try {
            f = new RandomAccessFile(fileName, mode);
            trace("openRandomAccessFile", fileName, f);
        } catch (IOException e) {
            freeMemoryAndFinalize();
            f = new RandomAccessFile(fileName, mode);
        }
        return new FileObjectDisk(f);
    }

}
