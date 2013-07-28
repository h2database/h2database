/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import org.h2.constant.ErrorCode;
import org.h2.message.DbException;
import org.h2.store.fs.FileUtils;

/**
 * This class allows to convert source code to a class. It uses one class loader
 * per class.
 */
public class SourceCompiler {

    private static final Class<?> JAVAC_SUN;

    /**
     * The class name to source code map.
     */
    final HashMap<String, String> sources = New.hashMap();

    /**
     * The class name to byte code map.
     */
    final HashMap<String, Class<?>> compiled = New.hashMap();

    private final String compileDir = Utils.getProperty("java.io.tmpdir", ".");

    static {
        Class<?> clazz;
        try {
            clazz = Class.forName("com.sun.tools.javac.Main");
        } catch (Exception e) {
            clazz = null;
        }
        JAVAC_SUN = clazz;
    }

    /**
     * Set the source code for the specified class.
     * This will reset all compiled classes.
     *
     * @param className the class name
     * @param source the source code
     */
    public void setSource(String className, String source) {
        sources.put(className, source);
        compiled.clear();
    }

    /**
     * Get the class object for the given name.
     *
     * @param packageAndClassName the class name
     * @return the class
     */
    public Class<?> getClass(String packageAndClassName) throws ClassNotFoundException {

        Class<?> compiledClass = compiled.get(packageAndClassName);
        if (compiledClass != null) {
            return compiledClass;
        }

        String source = sources.get(packageAndClassName);
        if (isGroovySource(source)) {
            Class<?> clazz = GroovyCompiler.parseClass(source, packageAndClassName);
            compiled.put(packageAndClassName, clazz);
            return clazz;
        }

        ClassLoader classLoader = new ClassLoader(getClass().getClassLoader()) {
            @Override
            public Class<?> findClass(String name) throws ClassNotFoundException {
                Class<?> classInstance = compiled.get(name);
                if (classInstance == null) {
                    String source = sources.get(name);
                    String packageName = null;
                    int idx = name.lastIndexOf('.');
                    String className;
                    if (idx >= 0) {
                        packageName = name.substring(0, idx);
                        className = name.substring(idx + 1);
                    } else {
                        className = name;
                    }
                    byte[] data = javacCompile(packageName, className, source);
                    if (data == null) {
                        classInstance = findSystemClass(name);
                    } else {
                        classInstance = defineClass(name, data, 0, data.length);
                        compiled.put(name, classInstance);
                    }
                }
                return classInstance;
            }
        };
        return classLoader.loadClass(packageAndClassName);
    }

    private static boolean isGroovySource(String source) {
        return source.startsWith("//groovy") || source.startsWith("@groovy");
    }

    /**
     * Get the first public static method of the given class.
     *
     * @param className the class name
     * @return the method name
     */
    public Method getMethod(String className) throws ClassNotFoundException {
        Class<?> clazz = getClass(className);
        Method[] methods = clazz.getDeclaredMethods();
        for (Method m : methods) {
            int modifiers = m.getModifiers();
            if (Modifier.isPublic(modifiers) && Modifier.isStatic(modifiers)) {
                String name = m.getName();
                if (!name.startsWith("_") && !m.getName().equals("main")) {
                    return m;
                }
            }
        }
        return null;
    }

    /**
     * Compile the given class. This method tries to use the class
     * "com.sun.tools.javac.Main" if available. If not, it tries to run "javac"
     * in a separate process.
     *
     * @param packageName the package name
     * @param className the class name
     * @param source the source code
     * @return the class file
     */
    byte[] javacCompile(String packageName, String className, String source) {
        File dir = new File(compileDir);
        if (packageName != null) {
            dir = new File(dir, packageName.replace('.', '/'));
            FileUtils.createDirectories(dir.getAbsolutePath());
        }
        File javaFile = new File(dir, className + ".java");
        File classFile = new File(dir, className + ".class");
        try {
            OutputStream f = FileUtils.newOutputStream(javaFile.getAbsolutePath(), false);
            PrintWriter out = new PrintWriter(IOUtils.getBufferedWriter(f));
            classFile.delete();
            if (source.startsWith("package ")) {
                out.println(source);
            } else {
                int endImport = source.indexOf("@CODE");
                String importCode = "import java.util.*;\n" +
                    "import java.math.*;\n" +
                    "import java.sql.*;\n";
                if (endImport >= 0) {
                    importCode = source.substring(0, endImport);
                    source = source.substring("@CODE".length() + endImport);
                }
                if (packageName != null) {
                    out.println("package " + packageName + ";");
                }
                out.println(importCode);
                out.println("public class "+ className +" {\n" +
                        "    public static " +
                        source + "\n" +
                        "}\n");
            }
            out.close();
            if (JAVAC_SUN != null) {
                javacSun(javaFile);
            } else {
                javacProcess(javaFile);
            }
            byte[] data = new byte[(int) classFile.length()];
            DataInputStream in = new DataInputStream(new FileInputStream(classFile));
            in.readFully(data);
            in.close();
            return data;
        } catch (Exception e) {
            throw DbException.convert(e);
        } finally {
            javaFile.delete();
            classFile.delete();
        }
    }

    private void javacProcess(File javaFile) {
        exec("javac",
                "-sourcepath", compileDir,
                "-d", compileDir,
                "-encoding", "UTF-8",
                javaFile.getAbsolutePath());
    }

    private int exec(String... args) {
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        try {
            ProcessBuilder builder = new ProcessBuilder();
            // The javac executable allows some of it's flags
            // to be smuggled in via environment variables.
            // But if it sees those flags, it will write out a message
            // to stderr, which messes up our parsing of the output.
            builder.environment().remove("JAVA_TOOL_OPTIONS");
            builder.command(args);

            Process p = builder.start();
            copyInThread(p.getInputStream(), buff);
            copyInThread(p.getErrorStream(), buff);
            p.waitFor();
            String err = new String(buff.toByteArray(), "UTF-8");
            throwSyntaxError(err);
            return p.exitValue();
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    private static void copyInThread(final InputStream in, final OutputStream out) {
        new Task() {
            @Override
            public void call() throws IOException {
                IOUtils.copy(in, out);
            }
        }.execute();
    }

    private void javacSun(File javaFile) {
        PrintStream old = System.err;
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        PrintStream temp = new PrintStream(buff);
        try {
            System.setErr(temp);
            Method compile;
            compile = JAVAC_SUN.getMethod("compile", String[].class);
            Object javac = JAVAC_SUN.newInstance();
            compile.invoke(javac, (Object) new String[] {
                    "-sourcepath", compileDir,
                    "-d", compileDir,
                    "-encoding", "UTF-8",
                    javaFile.getAbsolutePath() });
            String err = new String(buff.toByteArray(), "UTF-8");
            throwSyntaxError(err);
        } catch (Exception e) {
            throw DbException.convert(e);
        } finally {
            System.setErr(old);
        }
    }

    private void throwSyntaxError(String err) {
        if (err.startsWith("Note:")) {
            // unchecked or unsafe operations - just a warning
        } else if (err.length() > 0) {
            err = StringUtils.replaceAll(err, compileDir, "");
            throw DbException.get(ErrorCode.SYNTAX_ERROR_1, err);
        }
    }


    /**
     * Access the Groovy compiler using reflection, so that we do not gain a
     * compile-time dependency unnecessarily.
     */
    private static final class GroovyCompiler {
        private static final Object LOADER;
        private static final Throwable INIT_FAIL_EXCEPTION;

        static {
            Object tmpLoader = null;
            Throwable tmpInitFailException = null;
            try {
                // create an instance of ImportCustomizer
                Class<?> importCustomizerClass = Class.forName("org.codehaus.groovy.control.customizers.ImportCustomizer");
                Object importCustomizer = Utils.newInstance("org.codehaus.groovy.control.customizers.ImportCustomizer");
                // Call the method ImportCustomizer.addImports(String[])
                String[] importsArray = new String[] { "java.sql.Connection", "java.sql.Types", "java.sql.ResultSet",
                        "groovy.sql.Sql", "org.h2.tools.SimpleResultSet" };
                Utils.callMethod(importCustomizer, "addImports", new Object[] { importsArray });

                // Call the method
                // CompilerConfiguration.addCompilationCustomizers(ImportCustomizer...)
                Object importCustomizerArray = java.lang.reflect.Array.newInstance(importCustomizerClass, 1);
                java.lang.reflect.Array.set(importCustomizerArray, 0, importCustomizer);
                Object configuration = Utils.newInstance("org.codehaus.groovy.control.CompilerConfiguration");
                Utils.callMethod(configuration, "addCompilationCustomizers", new Object[] { importCustomizerArray });

                ClassLoader parent = GroovyCompiler.class.getClassLoader();
                tmpLoader = Utils.newInstance("groovy.lang.GroovyClassLoader", parent, configuration);
            } catch (Exception ex) {
                tmpInitFailException = ex;
            }
            LOADER = tmpLoader;
            INIT_FAIL_EXCEPTION = tmpInitFailException;
        }

        public static Class<?> parseClass(String source, String packageAndClassName) {
            if (LOADER == null) {
                throw new RuntimeException("compile fail: there is no groovy jar on the classpath?", INIT_FAIL_EXCEPTION);
            }
            try {
                Object codeSource = Utils.newInstance("groovy.lang.GroovyCodeSource", source, packageAndClassName
                        + ".groovy", "UTF-8");
                Utils.callMethod(codeSource, "setCachable", false);
                Class<?> clazz = (Class<?>) Utils.callMethod(LOADER, "parseClass", codeSource);
                return clazz;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}
