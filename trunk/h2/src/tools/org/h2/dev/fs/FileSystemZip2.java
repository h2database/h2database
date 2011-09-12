/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.store.fs.FileObject;
import org.h2.store.fs.FileObjectInputStream;
import org.h2.store.fs.FileSystem;
import org.h2.store.fs.FileSystemDisk;
import org.h2.store.fs.FileUtils;
import org.h2.util.New;

/**
 * This is a read-only file system that allows to access databases stored in a
 * .zip or .jar file. The problem of this file system is that data is always
 * accessed as a stream. But unlike FileSystemZip, it is possible to stack file
 * systems.
 */
public class FileSystemZip2 extends FileSystem {

    private static final String PREFIX = "zip2:";

    private static final FileSystemZip2 INSTANCE = new FileSystemZip2();

    /**
     * Register the file system.
     *
     * @return the instance
     */
    public static FileSystemZip2 register() {
        FileSystem.register(INSTANCE);
        return INSTANCE;
    }

    public boolean canWrite(String fileName) {
        return false;
    }

    public void createDirectory(String directoryName) {
        // ignore
    }

    public boolean createFile(String fileName) {
        throw DbException.getUnsupportedException("write");
    }

    public String createTempFile(String prefix, String suffix, boolean deleteOnExit, boolean inTempDir) throws IOException {
        if (!inTempDir) {
            throw new IOException("File system is read-only");
        }
        return FileSystemDisk.getInstance().createTempFile(prefix, suffix, deleteOnExit, true);
    }

    public void delete(String fileName) {
        throw DbException.getUnsupportedException("write");
    }

    public boolean exists(String fileName) {
        try {
            String entryName = getEntryName(fileName);
            if (entryName.length() == 0) {
                return true;
            }
            ZipInputStream file = openZip(fileName);
            boolean result = false;
            while (true) {
                ZipEntry entry = file.getNextEntry();
                if (entry == null) {
                    break;
                }
                if (entry.getName().equals(entryName)) {
                    result = true;
                    break;
                }
                file.closeEntry();
            }
            file.close();
            return result;
        } catch (IOException e) {
            return false;
        }
    }

    public boolean fileStartsWith(String fileName, String prefix) {
        return fileName.startsWith(prefix);
    }

    public String getName(String name) {
        name = getEntryName(name);
        if (name.endsWith("/")) {
            name = name.substring(0, name.length() - 1);
        }
        int idx = name.lastIndexOf('/');
        if (idx >= 0) {
            name = name.substring(idx + 1);
        }
        return name;
    }

    public long lastModified(String fileName) {
        return 0;
    }

    public String getParent(String fileName) {
        int idx = fileName.lastIndexOf('/');
        if (idx > 0) {
            fileName = fileName.substring(0, idx);
        }
        return fileName;
    }

    public boolean isAbsolute(String fileName) {
        return true;
    }

    public boolean isDirectory(String fileName) {
        try {
            String entryName = getEntryName(fileName);
            if (entryName.length() == 0) {
                return true;
            }
            ZipInputStream file = openZip(fileName);
            boolean result = false;
            while (true) {
                ZipEntry entry = file.getNextEntry();
                if (entry == null) {
                    break;
                }
                String n = entry.getName();
                if (n.equals(entryName)) {
                    result = entry.isDirectory();
                    break;
                } else  if (n.startsWith(entryName)) {
                    if (n.length() == entryName.length() + 1) {
                        if (n.equals(entryName + "/")) {
                            result = true;
                            break;
                        }
                    }
                }
                file.closeEntry();
            }
            file.close();
            return result;
        } catch (IOException e) {
            return false;
        }
    }

    public boolean isReadOnly(String fileName) {
        return true;
    }

    public boolean setReadOnly(String fileName) {
        return true;
    }

    public long size(String fileName) {
        try {
            String entryName = getEntryName(fileName);
            ZipInputStream file = openZip(fileName);
            long result = 0;
            while (true) {
                ZipEntry entry = file.getNextEntry();
                if (entry == null) {
                    break;
                }
                if (entry.getName().equals(entryName)) {
                    result = entry.getSize();
                    if (result == -1) {
                        result = 0;
                        while (true) {
                            long x = file.skip(16 * Constants.IO_BUFFER_SIZE);
                            if (x == 0) {
                                break;
                            }
                            result += x;
                        }
                    }
                    break;
                }
                file.closeEntry();
            }
            file.close();
            return result;
        } catch (IOException e) {
            return 0;
        }
    }

    public String[] listFiles(String path) {
        try {
            if (path.indexOf('!') < 0) {
                path += "!";
            }
            if (!path.endsWith("/")) {
                path += "/";
            }
            ZipInputStream file = openZip(path);
            String dirName = getEntryName(path);
            String prefix = path.substring(0, path.length() - dirName.length());
            ArrayList<String> list = New.arrayList();
            while (true) {
                ZipEntry entry = file.getNextEntry();
                if (entry == null) {
                    break;
                }
                String name = entry.getName();
                if (name.startsWith(dirName) && name.length() > dirName.length()) {
                    int idx = name.indexOf('/', dirName.length());
                    if (idx < 0 || idx >= name.length() - 1) {
                        list.add(prefix + name);
                    }
                }
                file.closeEntry();
            }
            file.close();
            String[] result = new String[list.size()];
            list.toArray(result);
            return result;
        } catch (IOException e) {
            throw DbException.convertIOException(e, "listFiles " + path);
        }
    }

    public String getCanonicalPath(String fileName) {
        return fileName;
    }

    public InputStream newInputStream(String fileName) throws IOException {
        FileObject file = openFileObject(fileName, "r");
        return new FileObjectInputStream(file);
    }

    public FileObject openFileObject(String fileName, String mode) throws IOException {
        String entryName = getEntryName(fileName);
        if (entryName.length() == 0) {
            throw new FileNotFoundException(fileName);
        }
        ZipInputStream in = openZip(fileName);
        while (true) {
            ZipEntry entry = in.getNextEntry();
            if (entry == null) {
                break;
            }
            if (entry.getName().equals(entryName)) {
                return new FileObjectZip2(fileName, entryName, in, size(fileName));
            }
            in.closeEntry();
        }
        in.close();
        throw new FileNotFoundException(fileName);
    }

    public OutputStream newOutputStream(String fileName, boolean append) {
        throw DbException.getUnsupportedException("write");
    }

    public void moveTo(String oldName, String newName) {
        throw DbException.getUnsupportedException("write");
    }

    public boolean tryDelete(String fileName) {
        return false;
    }

    public String unwrap(String fileName) {
        if (fileName.startsWith(PREFIX)) {
            fileName = fileName.substring(PREFIX.length());
        }
        int idx = fileName.indexOf('!');
        if (idx >= 0) {
            fileName = fileName.substring(0, idx);
        }
        return FileSystemDisk.expandUserHomeDirectory(fileName);
    }

    private static String getEntryName(String fileName) {
        int idx = fileName.indexOf('!');
        if (idx <= 0) {
            fileName = "";
        } else {
            fileName = fileName.substring(idx + 1);
        }
        fileName = fileName.replace('\\', '/');
        if (fileName.startsWith("/")) {
            fileName = fileName.substring(1);
        }
        return fileName;
    }

    private ZipInputStream openZip(String fileName) throws IOException {
        fileName = unwrap(fileName);
        return new ZipInputStream(FileUtils.newInputStream(fileName));
    }

    protected boolean accepts(String fileName) {
        return fileName.startsWith(PREFIX);
    }

}
