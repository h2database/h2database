/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (license2)
 * Initial Developer: H2 Group
 */
package org.h2.build;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.jar.JarOutputStream;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * This class is a complete pure Java build tool. It allows to build this
 * project without any external dependencies except a JDK.
 * Advantages: ability to debug the build, extensible, flexible, 
 * no XML, a bit faster.
 */
public class BuildBase {
    
    protected PrintStream out = System.out;
    
    protected void run(String[] args) {
        long time = System.currentTimeMillis();
        if (args.length == 0) {
            all();
        } else {
            for (int i = 0; i < args.length; i++) {
                String a = args[i];
                Method m = null;
                try {
                    m = getClass().getMethod(a, new Class[0]);
                } catch (Exception e) {
                    out.println("Unknown target: " + a);
                    projectHelp();
                    break;
                }
                out.println("Running target " + a);
                try {
                    try {
                        m.invoke(this, new Object[0]);
                    } catch (InvocationTargetException e) {
                        throw e.getTargetException();
                    }
                } catch (Throwable e) {
                    throw new Error(e);
                }
            }
        }
        out.println("Done in " + (System.currentTimeMillis() - time) + " ms");
        
    }
    
    protected void all() {
        projectHelp();
    }
    
    protected void projectHelp() {
        Method[] methods = getClass().getDeclaredMethods();
        out.println("Targets:");
        for (int i = 0; i < methods.length; i++) {
            Method m = methods[i];
            if ((m.getModifiers() & Modifier.STATIC) == 0 && m.getParameterTypes().length == 0) {
                out.println(m.getName());
            }
        }
        out.println();
    }

    private int exec(String command, String[] args) {
        try {
            out.print(command);
            for (int i = 0; args != null && i < args.length; i++) {
                out.print(" ");
                out.print(args[i]);
            }
            out.println();
            Process p = Runtime.getRuntime().exec(command, args);
            copy(p.getInputStream(), out);
            copy(p.getErrorStream(), out);
            return p.exitValue();
        } catch (Exception e) {
            throw new Error("Error: " + e, e);
        }
    }
    
    private void copy(InputStream in, OutputStream out) throws IOException {
        while (true) {
            int x = in.read();
            if (x < 0) {
                return;
            }
            out.write(x);
        }
    }
    
    protected String getStaticField(String className, String fieldName) {
        try {
            Class clazz = Class.forName(className);
            Field field = clazz.getField(fieldName);
            return field.get(null).toString();
        } catch (Exception e) {
            throw new Error("Can not read field " + className + "." + fieldName, e);
        }
    }
    
    protected void copy(String targetDir, List files, String baseDir) {
        File target = new File(targetDir);
        File base = new File(baseDir);
        out.println("Copying " + files.size() + " files to " + target.getPath());
        String basePath = base.getPath();
        for (int i = 0; i < files.size(); i++) {
            File f = (File) files.get(i);
            File t = new File(target, removeBase(basePath, f.getPath()));
            byte[] data = readFile(f);
            t.getParentFile().mkdirs();
            writeFile(t, data);
        }
    }
    
    protected void javadoc(String[] args) {
        int result;        
        try {
            Class clazz = Class.forName("com.sun.tools.javadoc.Main");
            Method execute = clazz.getMethod("execute", new Class[] { String[].class });
            result = ((Integer) execute.invoke(null, new Object[] { args })).intValue();
        } catch (Exception e) {
            result = exec("javadoc", args);
        }
        if (result != 0) {
            throw new Error("An error occured");
        }
    }
    
    private String convertBytesToString(byte[] value) {
        StringBuffer buff = new StringBuffer(value.length * 2);
        for (int i = 0; i < value.length; i++) {
            int c = value[i] & 0xff;
            buff.append(Integer.toString(c >> 4, 16));
            buff.append(Integer.toString(c & 0xf, 16));
        }
        return buff.toString();
    }
    
    private String getSHA1(byte[] data) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-1");
            return convertBytesToString(md.digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new Error(e);
        } 
    }
    
    protected void download(String target, String fileURL, String sha1Checksum) {
        File targetFile = new File(target);
        if (targetFile.exists()) {
            return;
        }
        targetFile.getParentFile().mkdirs();
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        try {
            out.println("Downloading " + fileURL);
            URL url = new URL(fileURL);
            InputStream in = new BufferedInputStream(url.openStream());
            long last = System.currentTimeMillis();
            int len = 0;
            while (true) {
                long now = System.currentTimeMillis();
                if (now > last + 1000) {
                    out.println("Downloaded " + len + " bytes");
                    last = now;
                }
                int x = in.read();
                len++;
                if (x < 0) {
                    break;
                }
                buff.write(x);
            }
        } catch (IOException e) {
            throw new Error("Error downloading", e);
        }
        byte[] data = buff.toByteArray();
        String got = getSHA1(data);
        if (sha1Checksum == null) {
            out.println("SHA1 checksum: " + got);
        } else {
            
            if (!got.equals(sha1Checksum)) {
                throw new Error("SHA1 checksum mismatch");
            }
        }
        writeFile(targetFile, data);
    }
    
    protected List getFiles(String path) {
        ArrayList list = new ArrayList();
        addFiles(list, new File(path));
        return list;
    }
    
    private void addFiles(List list, File file) {
        if (file.getName().startsWith(".svn")) {
        } else if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                addFiles(list, files[i]);
            }
        } else {
            list.add(file);
        }
    }
    
    protected List filterFiles(List files, boolean keep, String mask) {
        boolean start = false;
        if (mask.endsWith("*")) {
            mask = mask.substring(0, mask.length() - 1);
            start = true;
        } else if (mask.startsWith("*")) {
            mask = mask.substring(1);
        }
        if (mask.indexOf('*') >= 0) {
            throw new Error("Unsupported mask, may only start or end with *:" + mask);
        }
        // normalize / and \
        mask = new File(mask).getPath();
        ArrayList list = new ArrayList();
        for (int i = 0; i < files.size(); i++) {
            File f = (File) files.get(i);
            String path = f.getPath();
            boolean match = start ? path.startsWith(mask) : path.endsWith(mask);
            if (match == keep) {
                list.add(f);
            }
        }
        return list;
    }
    
    private String removeBase(String basePath, String path) {
        if (path.startsWith(basePath)) {
            path = path.substring(basePath.length());
        }
        path = path.replace('\\', '/');
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        return path;
    }
    
    protected void writeFile(File file, byte[] data) {
        try {
            RandomAccessFile ra = new RandomAccessFile(file, "rw");
            ra.write(data);
            ra.close();
        } catch (IOException e) {
            throw new Error("Error writing to file " + file, e);
        }
    }
    
    protected byte[] readFile(File file) {
        try {
            RandomAccessFile ra = new RandomAccessFile(file, "r");
            long len = ra.length();
            if (len >= Integer.MAX_VALUE) {
                throw new Error("File " + file.getPath() + " is too large");
            }
            byte[] buffer = new byte[(int) len];
            ra.readFully(buffer);
            ra.close();
            return buffer;
        } catch (IOException e) {
            throw new Error("Error reading from file " + file, e);
        }
    }
    
    private String getSuffix(String fileName) {
        int idx = fileName.lastIndexOf('.');
        return idx < 0 ? "" : fileName.substring(idx);
    }
    
    protected void jar(String destFile, String basePath, List files) {
        zipOrJar(destFile, basePath, files, false, false, true);
    }

    protected void zip(String destFile, String basePath, List files, boolean storeOnly, boolean sortBySuffix) {
        zipOrJar(destFile, basePath, files, storeOnly, sortBySuffix, false);
    }

    protected void zipOrJar(String destFile, String basePath, List files, boolean storeOnly, boolean sortBySuffix, boolean jar) {
        if (sortBySuffix) {
            // for better compressibility, sort by suffix, then name
            Collections.sort(files, new Comparator() {
                public int compare(Object o1, Object o2) {
                    String p1 = ((File) o1).getPath();
                    String p2 = ((File) o2).getPath();
                    int comp = getSuffix(p1).compareTo(getSuffix(p2));
                    if (comp == 0) {
                        comp = p1.compareTo(p2);
                    }
                    return comp;
                }
            });
        }
        new File(destFile).getParentFile().mkdirs();
        // normalize the path (replace / with \ if required)
        basePath = new File(basePath).getPath();
        try {
            OutputStream out = new BufferedOutputStream(new FileOutputStream(destFile));
            ZipOutputStream zipOut;
            if (jar) {
                zipOut = new JarOutputStream(out);
            } else {
                zipOut = new ZipOutputStream(out);
            }
            if (storeOnly) {
                zipOut.setMethod(ZipOutputStream.STORED);
            }
            zipOut.setLevel(Deflater.BEST_COMPRESSION);
            for (int i = 0; i < files.size(); i++) {
                File file = (File) files.get(i);
                String fileName = file.getPath();
                String entryName = removeBase(basePath, fileName);
                byte[] data = readFile(file);
                ZipEntry entry = new ZipEntry(entryName);
                CRC32 crc = new CRC32();
                crc.update(data);
                entry.setSize(file.length());
                entry.setCrc(crc.getValue());
                zipOut.putNextEntry(entry);
                zipOut.write(data);
                zipOut.closeEntry();
            }
            zipOut.closeEntry();
            zipOut.close();
        } catch (IOException e) {
            throw new Error("Error creating file " + destFile, e);
        }
    }
    
    protected String getJavaSpecVersion() {
        return System.getProperty("java.specification.version");
    }
    
    private List getPaths(List files) {
        ArrayList list = new ArrayList(files.size());
        for (int i = 0; i < files.size(); i++) {
            list.add(((File) files.get(i)).getPath());
        }
        return list;
    }
    
    protected void javac(String[] args, List files) {
        out.println("Compiling " + files.size() + " classes");
        ArrayList argList = new ArrayList(Arrays.asList(args));
        argList.addAll(getPaths(filterFiles(files, true, ".java")));
        args = new String[argList.size()];
        argList.toArray(args);
        int result;
        try {
            Class clazz = Class.forName("com.sun.tools.javac.Main");
            Method compile = clazz.getMethod("compile", new Class[] { String[].class });
            Object instance = clazz.newInstance();
            result = ((Integer) compile.invoke(instance, new Object[] { args })).intValue();
        } catch (Exception e) {
            e.printStackTrace();
            result = exec("javac", args);
        }
        if (result != 0) {
            throw new Error("An error occured");
        }
    }
    
    protected void java(String className, String[] args) {
        out.println("Executing " + className);
        try {
            Method main = Class.forName(className).getMethod("main", new Class[] { String[].class });
            main.invoke(null, new Object[] { args });
        } catch (Exception e) {
            throw new Error(e);
        }
    }
    
    protected void mkdir(String dir) {
        if (!new File(dir).mkdirs()) {
            throw new Error("Can not create directory " + dir);
        }
    }
    
    protected void delete(String path) {
        out.println("Deleting " + path);
        delete(new File(path));
    }
    
    private void delete(File f) {
        if (f.exists()) {
            if (f.isDirectory()) {
                File[] list = f.listFiles();
                for (int i = 0; i < list.length; i++) {
                    delete(list[i]);
                }
            }
            if (!f.delete()) {
                throw new Error("Can not delete " + f.getPath());
            }
        }
    }
    
    protected String replaceAll(String s, String before, String after) {
        int index = 0;
        while (true) {
            int next = s.indexOf(before, index);
            if (next < 0) {
                return s;
            }
            s = s.substring(0, next) + after + s.substring(next + before.length());
            index = next + after.length();
        }
    }
}
