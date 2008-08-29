/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
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
    
    /**
     * A list of files.
     */
    public static class FileList extends ArrayList implements List {
        
        private static final long serialVersionUID = -3241001695597802578L;

        /**
         * Remove the files that match from the list.
         * Patterns must start or end with a *.
         * 
         * @param pattern the pattern of the file names to remove
         * @return the new file list
         */
        public FileList exclude(String pattern) {
            return filterFiles(this, false, pattern);
        }
        
        /**
         * Only keep the files that match.
         * Patterns must start or end with a *.
         * 
         * @param pattern the pattern of the file names to keep
         * @return the new file list
         */
        public FileList keep(String pattern) {
            return filterFiles(this, true, pattern);
        }
        
    }
    
    /**
     * The output stream (System.out).
     */
    protected PrintStream out = System.out;
    
    /**
     * If output should be disabled.
     */
    protected boolean quiet;
    
    /**
     * This method should be called by the main method.
     * 
     * @param args the command line parameters
     */
    protected void run(String[] args) {
        long time = System.currentTimeMillis();
        if (args.length == 0) {
            all();
        } else {
            for (int i = 0; i < args.length; i++) {
                String a = args[i];
                if ("-quiet".equals(a)) {
                    quiet = true;
                } else {
                    Method m = null;
                    try {
                        m = getClass().getMethod(a, new Class[0]);
                    } catch (Exception e) {
                        out.println("Unknown target: " + a);
                        projectHelp();
                        break;
                    }
                    println("Target: " + a);
                    invoke(m, this, new Object[0]);
                }
            }
        }
        println("Done in " + (System.currentTimeMillis() - time) + " ms");
    }
    
    private Object invoke(Method m, Object instance, Object[] args) {
        try {
            try {
                return m.invoke(instance, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        } catch (Throwable e) {
            throw e instanceof Error ? ((Error) e) : new Error(e);
        }
    }
    
    /**
     * This method is called if no other target is specified in the command line.
     * The default behavior is to call projectHelp().
     * Override this method if you want another default behavior.
     */
    protected void all() {
        projectHelp();
    }
    
    /**
     * Emit a beep.
     */
    public void beep() {
        out.print("\007");
        out.flush();
    }

    /**
     * Lists all targets (all public methods non-static methods without
     * parameters).
     */
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
    
    private boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().indexOf("windows") >= 0;
    }
    
    /**
     * Execute a script in a separate process.
     * In Windows, the batch file with this name (.bat) is run.
     *
     * @param script the program to run
     * @param args the command line parameters
     * @return the exit value
     */
    protected int execScript(String script, String[] args) {
        if (isWindows()) {
            script = script + ".bat";
        }
        return exec(script, args);
    }

    /**
     * Execute a program in a separate process.
     * 
     * @param command the program to run
     * @param args the command line parameters
     * @return the exit value
     */
    protected int exec(String command, String[] args) {
        try {
            print(command);
            for (int i = 0; args != null && i < args.length; i++) {
                print(" " + args[i]);
            }
            println("");
            String[] cmdArray = new String[1 + (args == null ? 0 : args.length)];
            cmdArray[0] = command;
            if (args != null) {
                System.arraycopy(args, 0, cmdArray, 1, args.length);
            }
            Process p = Runtime.getRuntime().exec(cmdArray);
            copyInThread(p.getInputStream(), quiet ? null : out);
            copyInThread(p.getErrorStream(), quiet ? null : out);
            p.waitFor();
            return p.exitValue();
        } catch (Exception e) {
            throw new Error("Error: " + e, e);
        }
    }
    
    private void copyInThread(final InputStream in, final OutputStream out) {
        new Thread() {
            public void run() {
                try {
                    while (true) {
                        int x = in.read();
                        if (x < 0) {
                            return;
                        }
                        if (out != null) {
                            out.write(x);
                        }
                    }
                } catch (Exception e) {
                    throw new Error("Error: " + e, e);
                }
            }
        } .start();
    }
    
    /**
     * Read a final static field in a class using reflection.
     * 
     * @param className the name of the class
     * @param fieldName the field name
     * @return the value as a string
     */
    protected String getStaticField(String className, String fieldName) {
        try {
            Class clazz = Class.forName(className);
            Field field = clazz.getField(fieldName);
            return field.get(null).toString();
        } catch (Exception e) {
            throw new Error("Can not read field " + className + "." + fieldName, e);
        }
    }

    /**
     * Reads the value from a static method of a class using reflection.
     * 
     * @param className the name of the class
     * @param methodName the field name
     * @return the value as a string
     */
    protected String getStaticValue(String className, String methodName) {
        try {
            Class clazz = Class.forName(className);
            Method method = clazz.getMethod(methodName, new Class[0]);
            return method.invoke(null, new Object[0]).toString();
        } catch (Exception e) {
            throw new Error("Can not read value " + className + "." + methodName + "()", e);
        }
    }

    /**
     * Copy files to the specified target directory.
     * 
     * @param targetDir the target directory
     * @param files the list of files to copy
     * @param baseDir the base directory
     */
    protected void copy(String targetDir, List files, String baseDir) {
        File target = new File(targetDir);
        File base = new File(baseDir);
        println("Copying " + files.size() + " files to " + target.getPath());
        String basePath = base.getPath();
        for (int i = 0; i < files.size(); i++) {
            File f = (File) files.get(i);
            File t = new File(target, removeBase(basePath, f.getPath()));
            byte[] data = readFile(f);
            t.getParentFile().mkdirs();
            writeFile(t, data);
        }
    }
    
    private PrintStream filter(PrintStream out, final String[] exclude) {
        return new PrintStream(new FilterOutputStream(out) {
            private ByteArrayOutputStream buff = new ByteArrayOutputStream();

            public void write(byte[] b) throws IOException {
                write(b, 0, b.length);
            }

            public void write(byte[] b, int off, int len) throws IOException {
                for (int i = off; i < len; i++) {
                    write(b[i]);
                }
            }

            public void write(byte b) throws IOException {
                buff.write(b);
                if (b == '\n') {
                    byte[] data = buff.toByteArray();
                    String line = new String(data, "UTF-8");
                    boolean print = true;
                    for (int i = 0; i < exclude.length; i++) {
                        if (line.startsWith(exclude[i])) {
                            print = false;
                            break;
                        }
                    }
                    if (print) {
                        out.write(data);
                    }
                    buff.reset();
                }
            }

            public void close() throws IOException {
                write('\n');
            }
        });
    }
    
    /**
     * Run a Javadoc task.
     * 
     * @param args the command line arguments to pass
     */
    protected void javadoc(String[] args) {
        int result;        
        PrintStream old = System.out;
        try {
            println("Javadoc");
            if (quiet) {
                System.setOut(filter(System.out, new String[] {
                        "Loading source files for package",
                        "Constructing Javadoc information",
                        "Generating ",
                        "Standard Doclet",
                        "Building "
                }));
            }
            Class clazz = Class.forName("com.sun.tools.javadoc.Main");
            Method execute = clazz.getMethod("execute", new Class[] { String[].class });
            result = ((Integer) invoke(execute, null, new Object[] { args })).intValue();
        } catch (Exception e) {
            result = exec("javadoc", args);
        } finally {
            System.setOut(old);
        }
        if (result != 0) {
            throw new Error("An error occurred");
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
    
    /**
     * Download a file if it does not yet exist.
     * If no checksum is used (that is, if the parameter is null), the
     * checksum is printed. For security, checksums should always be used.
     * 
     * @param target the target file name
     * @param fileURL the source url of the file
     * @param sha1Checksum the SHA-1 checksum or null
     */
    protected void download(String target, String fileURL, String sha1Checksum) {
        File targetFile = new File(target);
        if (targetFile.exists()) {
            return;
        }
        targetFile.getAbsoluteFile().getParentFile().mkdirs();
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        try {
            println("Downloading " + fileURL);
            URL url = new URL(fileURL);
            InputStream in = new BufferedInputStream(url.openStream());
            long last = System.currentTimeMillis();
            int len = 0;
            while (true) {
                long now = System.currentTimeMillis();
                if (now > last + 1000) {
                    println("Downloaded " + len + " bytes");
                    last = now;
                }
                int x = in.read();
                len++;
                if (x < 0) {
                    break;
                }
                buff.write(x);
            }
            in.close();
        } catch (IOException e) {
            throw new Error("Error downloading", e);
        }
        byte[] data = buff.toByteArray();
        String got = getSHA1(data);
        if (sha1Checksum == null) {
            println("SHA1 checksum: " + got);
        } else {
            
            if (!got.equals(sha1Checksum)) {
                throw new Error("SHA1 checksum mismatch");
            }
        }
        writeFile(targetFile, data);
    }
    
    /**
     * Get the list of files in the given directory and all subdirectories.
     * 
     * @param dir the source directory
     * @return the file list
     */
    protected FileList getFiles(String dir) {
        FileList list = new FileList();
        addFiles(list, new File(dir));
        return list;
    }
    
    private void addFiles(List list, File file) {
        if (file.getName().startsWith(".svn")) {
            // ignore
        } else if (file.isDirectory()) {
            String[] fileNames = file.list();
            String path = file.getPath();
            for (int i = 0; i < fileNames.length; i++) {
                addFiles(list, new File(path, fileNames[i]));
            }
        } else {
            list.add(file);
        }
    }
    
    /**
     * Filter a list of file names.
     * 
     * @param files the original list
     * @param keep if matching file names should be kept or removed
     * @param pattern the file name pattern
     * @return the filtered file list
     */
    static FileList filterFiles(FileList files, boolean keep, String pattern) {
        boolean start = false;
        if (pattern.endsWith("*")) {
            pattern = pattern.substring(0, pattern.length() - 1);
            start = true;
        } else if (pattern.startsWith("*")) {
            pattern = pattern.substring(1);
        }
        if (pattern.indexOf('*') >= 0) {
            throw new Error("Unsupported pattern, may only start or end with *:" + pattern);
        }
        // normalize / and \
        pattern = replaceAll(pattern, "/", File.separator);
        FileList list = new FileList();
        for (int i = 0; i < files.size(); i++) {
            File f = (File) files.get(i);
            String path = f.getPath();
            boolean match = start ? path.startsWith(pattern) : path.endsWith(pattern);
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
    
    /**
     * Create or overwrite a file.
     * 
     * @param file the file
     * @param data the data to write
     */
    public static void writeFile(File file, byte[] data) {
        try {
            RandomAccessFile ra = new RandomAccessFile(file, "rw");
            ra.write(data);
            ra.setLength(data.length);
            ra.close();
        } catch (IOException e) {
            throw new Error("Error writing to file " + file, e);
        }
    }
    
    /**
     * Read a file. The maximum file size is Integer.MAX_VALUE.
     * 
     * @param file the file
     * @return the data
     */
    public static byte[] readFile(File file) {
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
    
    /**
     * Get the file name suffix.
     * 
     * @param fileName the file name
     * @return the suffix or an empty string if there is none
     */
    String getSuffix(String fileName) {
        int idx = fileName.lastIndexOf('.');
        return idx < 0 ? "" : fileName.substring(idx);
    }
    
    /**
     * Create a jar file.
     * 
     * @param destFile the target file name
     * @param files the file list
     * @param basePath the base path
     * @return the size of the jar file in KB
     */
    protected long jar(String destFile, List files, String basePath) {
        long kb = zipOrJar(destFile, files, basePath, false, false, true);
        println("Jar " + destFile + " (" + kb + " KB)");
        return kb;
    }

    /**
     * Create a zip file.
     * 
     * @param destFile the target file name
     * @param files the file list
     * @param basePath the base path
     * @param storeOnly if the files should not be compressed
     * @param sortBySuffix if the file should be sorted by the file suffix
     */
    protected void zip(String destFile, List files, String basePath, boolean storeOnly, boolean sortBySuffix) {
        long kb = zipOrJar(destFile, files, basePath, storeOnly, sortBySuffix, false);
        println("Zip " + destFile + " (" + kb + " KB)");
    }

    private long zipOrJar(String destFile, List files, String basePath, boolean storeOnly, boolean sortBySuffix, boolean jar) {
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
        new File(destFile).getAbsoluteFile().getParentFile().mkdirs();
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
            return new File(destFile).length() / 1024;
        } catch (IOException e) {
            throw new Error("Error creating file " + destFile, e);
        }
    }
    
    /**
     * Get the current java specification version (for example, 1.4).
     * 
     * @return the java specification version
     */
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
    
    /**
     * Compile the files.
     * 
     * @param args the command line parameters
     * @param files the file list
     */
    protected void javac(String[] args, FileList files) {
        println("Compiling " + files.size() + " classes");
        ArrayList argList = new ArrayList(Arrays.asList(args));
        argList.addAll(getPaths(filterFiles(files, true, ".java")));
        args = new String[argList.size()];
        argList.toArray(args);
        int result;
        PrintStream old = System.err;
        try {
            if (quiet) {
                System.setErr(filter(System.err, new String[] {
                        "Note:"
                }));
            }
            Class clazz = Class.forName("com.sun.tools.javac.Main");
            Method compile = clazz.getMethod("compile", new Class[] { String[].class });
            Object instance = clazz.newInstance();
            result = ((Integer) invoke(compile, instance, new Object[] { args })).intValue();
        } catch (Exception e) {
            e.printStackTrace();
            result = exec("javac", args);
        } finally {
            System.setErr(old);
        }
        if (result != 0) {
            throw new Error("An error occurred");
        }
    }
    
    /**
     * Call the main method of the given Java class using reflection.
     * 
     * @param className the class name
     * @param args the command line parameters to pass
     */
    protected void java(String className, String[] args) {
        println("Running " + className);
        if (args == null) {
            args = new String[0];
        }
        try {
            Method main = Class.forName(className).getMethod("main", new Class[] { String[].class });
            invoke(main, null, new Object[] { args });
        } catch (Exception e) {
            throw new Error(e);
        }
    }
    
    /**
     * Create the directory including the parent directories if they don't exist.
     * 
     * @param dir the directory to create
     */
    protected void mkdir(String dir) {
        File f = new File(dir);
        if (f.exists()) {
            if (f.isFile()) {
                throw new Error("Can not create directory " + dir + " because a file with this name exists");
            }
        } else {
            if (!f.mkdirs()) {
                throw new Error("Can not create directory " + dir);
            }                
        }
    }
    
    /**
     * Delete all files in the given directory and all subdirectories.
     *  
     * @param dir the name of the directory
     */
    protected void delete(String dir) {
        println("Deleting " + dir);
        delete(new File(dir));
    }
    
    private void delete(File file) {
        if (file.exists()) {
            if (file.isDirectory()) {
                String[] fileNames = file.list();
                String path = file.getPath();
                for (int i = 0; i < fileNames.length; i++) {
                    delete(new File(path, fileNames[i]));
                }
            }
            if (!file.delete()) {
                throw new Error("Can not delete " + file.getPath());
            }
        }
    }
    
    /**
     * Replace each substring in a given string. Regular expression is not used.
     * 
     * @param s the original text
     * @param before the old substring
     * @param after the new substring
     * @return the string with the string replaced
     */
    protected static String replaceAll(String s, String before, String after) {
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
    
    /**
     * Print a line to the output unless the quiet mode is enabled.
     * 
     * @param s the text to write
     */
    protected void println(String s) {
        if (!quiet) {
            out.println(s);
        }
    }
    
    /**
     * Print a message to the output unless the quiet mode is enabled.
     * 
     * @param s the message to write
     */
    protected void print(String s) {
        if (!quiet) {
            out.print(s);
        }
    }

}
