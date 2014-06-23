/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.h2.build.code.SwitchSource;
import org.h2.build.doc.XMLParser;

/**
 * The build definition.
 */
public class Build extends BuildBase {

    private boolean filesMissing;

    /**
     * Run the build.
     *
     * @param args the command line arguments
     */
    public static void main(String... args) {
        new Build().run(args);
    }

    /**
     * Run the benchmarks.
     */
    public void benchmark() {
        downloadUsingMaven("ext/hsqldb-2.0.0.jar", "hsqldb", "hsqldb", "2.0.0",
                "c9d525ce1a464185e6b01c7de872127a06092673");
        downloadUsingMaven("ext/derby-10.6.1.0.jar", "org/apache/derby", "derby", "10.6.1.0",
                "01137cd636b0e3c22f0d273478adb58aa30e984a");
        downloadUsingMaven("ext/derbyclient-10.6.1.0.jar", "org/apache/derby", "derbyclient", "10.6.1.0",
                "e7c6fbaca2ef4dbcad27fa7d8a9cd1ac0d1e4b00");
        downloadUsingMaven("ext/derbynet-10.6.1.0.jar", "org/apache/derby", "derbynet", "10.6.1.0",
                "d5d9d7b783eeaef016be85c34d5c65d1e7cec764");
        downloadUsingMaven("ext/postgresql-8.3-603.jdbc3.jar", "postgresql", "postgresql", "8.3-603.jdbc3",
                "33d531c3c53055ddcbea3d88bfa093466ffef924");
        downloadUsingMaven("ext/mysql-connector-java-5.1.6.jar", "mysql", "mysql-connector-java", "5.1.6",
                "380ef5226de2c85ff3b38cbfefeea881c5fce09d");
        compile();

        String cp = "temp" + File.pathSeparator + "bin/h2" + getJarSuffix() + File.pathSeparator +
        "ext/hsqldb.jar" + File.pathSeparator +
        "ext/hsqldb-2.0.0.jar" + File.pathSeparator +
        "ext/derby-10.6.1.0.jar" + File.pathSeparator +
        "ext/derbyclient-10.6.1.0.jar" + File.pathSeparator +
        "ext/derbynet-10.6.1.0.jar" + File.pathSeparator +
        "ext/postgresql-8.3-603.jdbc3.jar" + File.pathSeparator +
        "ext/mysql-connector-java-5.1.6.jar";
        StringList args = args("-Xmx128m", "-cp", cp, "org.h2.test.bench.TestPerformance");
        exec("java", args.plus("-init", "-db", "1"));
        exec("java", args.plus("-db", "2"));
        exec("java", args.plus("-db", "3", "-out", "pe.html"));
        exec("java", args.plus("-init", "-db", "4"));
        exec("java", args.plus("-db", "5", "-exit"));
        exec("java", args.plus("-db", "6"));
        exec("java", args.plus("-db", "7"));
        exec("java", args.plus("-db", "8", "-out", "ps.html"));
    }

    /**
     * Clean all jar files, classes, and generated documentation.
     */
    public void clean() {
        delete("temp");
        delete("docs");
        mkdir("docs");
        mkdir("bin");
        delete(files(".").keep("*/Thumbs.db"));
    }

    /**
     * Compile all classes
     */
    public void compile() {
        compile(true, false, false);
    }

    private void compileTools() {
        FileList files = files("src/tools").keep("src/tools/org/h2/build/*");
        StringList args = args("-d", "temp", "-sourcepath", "src/tools" +
                File.pathSeparator + "src/test" + File.pathSeparator + "src/main");
        mkdir("temp");
        javac(args, files);
    }

    /**
     * Run the Emma code coverage.
     */
    public void coverage() {
        downloadTest();
        downloadUsingMaven("ext/emma-2.0.5312.jar",
                "emma", "emma", "2.0.5312",
                "30a40933caf67d88d9e75957950ccf353b181ab7");
        String cp = "temp" + File.pathSeparator + "bin" +
            File.pathSeparator + "ext/emma-2.0.5312.jar" +
            File.pathSeparator + "ext/postgresql-8.3-603.jdbc3.jar" +
            File.pathSeparator + "ext/servlet-api-2.4.jar" +
            File.pathSeparator + "ext/" + getLuceneJar() +
            File.pathSeparator + "ext/h2mig_pagestore_addon.jar" +
            File.pathSeparator + "ext/org.osgi.core-4.2.0.jar" +
            File.pathSeparator + "ext/org.osgi.enterprise-4.2.0.jar" +
            File.pathSeparator + "ext/jts-1.13.jar" +
            File.pathSeparator + "ext/slf4j-api-1.6.0.jar" +
            File.pathSeparator + "ext/slf4j-nop-1.6.0.jar" +
            File.pathSeparator + System.getProperty("java.home") + "/../lib/tools.jar";
        // -XX:-UseSplitVerifier is for Java 7 compatibility
        exec("java", args(
                "-Xmx128m",
                "-XX:-UseSplitVerifier",
                "-cp", cp, "emma", "run",
                "-cp", "temp",
                "-sp", "src/main",
                "-r", "html,txt",
                "-ix", "-org.h2.test.*,-org.h2.dev.*,-org.h2.jaqu.*,-org.h2.mode.*,-org.h2.server.pg.*",
                "org.h2.test.TestAll"));
    }

    /**
     * Switch the source code to the current JDK.
     */
    public void switchSource() {
        switchSource(true);
    }

    private static void switchSource(boolean enableCheck) {
        try {
            String version = System.getProperty("version");
            String check = enableCheck ? "+CHECK" : "-CHECK";
            if (version == null) {
                SwitchSource.main("-dir", "src", "-auto", check);
            } else {
                SwitchSource.main("-dir", "src", "-version", version, check);
            }
            SwitchSource.main("-dir", "src", "-LUCENE2", "-LUCENE3", "+LUCENE" + getLuceneVersion());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void compileMVStore(boolean debugInfo) {
        switchSource(debugInfo);
        clean();
        mkdir("temp");
        download();
        String classpath = "temp";
        FileList files;
        files = files("src/main/org/h2/mvstore").
                exclude("src/main/org/h2/mvstore/db/*");
        StringList args = args();
        if (debugInfo) {
            args = args.plus("-Xlint:unchecked", "-d", "temp", "-sourcepath", "src/main", "-classpath", classpath);
        } else {
            args = args.plus("-Xlint:unchecked", "-g:none", "-d", "temp", "-sourcepath", "src/main", "-classpath", classpath);
        }
        System.out.println(files);
        javac(args, files);
    }

    private void compile(boolean debugInfo, boolean clientOnly, boolean basicResourcesOnly) {
        switchSource(debugInfo);
        clean();
        mkdir("temp");
        download();
        String classpath = "temp" +
                File.pathSeparator + "ext/servlet-api-2.4.jar" +
                File.pathSeparator + "ext/" + getLuceneJar() +
                File.pathSeparator + "ext/slf4j-api-1.6.0.jar" +
                File.pathSeparator + "ext/org.osgi.core-4.2.0.jar" +
                File.pathSeparator + "ext/org.osgi.enterprise-4.2.0.jar" +
                File.pathSeparator + "ext/jts-1.13.jar" +
                File.pathSeparator + System.getProperty("java.home") + "/../lib/tools.jar";
        FileList files;
        if (clientOnly) {
            files = files("src/main/org/h2/Driver.java");
            files.addAll(files("src/main/org/h2/jdbc"));
            files.addAll(files("src/main/org/h2/jdbcx"));
        } else {
            files = files("src/main");
        }
        StringList args = args();
        if (System.getProperty("version") != null) {
            String bcp = System.getProperty("bcp");
            // /System/Library/Frameworks/JavaVM.framework/Versions/1.4/Classes/classes.jar
            args = args.plus("-source", "1.5", "-target", "jsr14", "-bootclasspath", bcp);
        }
        if (debugInfo) {
            args = args.plus("-Xlint:unchecked", "-d", "temp", "-sourcepath", "src/main", "-classpath", classpath);
        } else {
            args = args.plus("-Xlint:unchecked", "-g:none", "-d", "temp", "-sourcepath", "src/main", "-classpath", classpath);
        }
        javac(args, files);

        files = files("src/main/META-INF/services");
        copy("temp", files, "src/main");

        if (!clientOnly) {
            files = files("src/test");
            files.addAll(files("src/tools"));
            args = args("-Xlint:unchecked", "-d", "temp", "-sourcepath", "src/test" + File.pathSeparator + "src/tools",
                    "-classpath", classpath);
            javac(args, files);
            files = files("src/test").
                exclude("*.java").
                exclude("*/package.html");
            copy("temp", files, "src/test");
        }
        resources(clientOnly, basicResourcesOnly);
    }

    private static void filter(String source, String target, String old, String replacement) {
        String text = new String(readFile(new File(source)));
        text = replaceAll(text, old, replacement);
        writeFile(new File(target), text.getBytes());
    }

    /**
     * Create the documentation from the documentation sources. API Javadocs are
     * created as well.
     */
    public void docs() {
        javadoc();
        copy("docs", files("src/docsrc/index.html"), "src/docsrc");
        java("org.h2.build.doc.XMLChecker", null);
        java("org.h2.build.code.CheckJavadoc", null);
        java("org.h2.build.code.CheckTextFiles", null);
        java("org.h2.build.doc.GenerateDoc", null);
        java("org.h2.build.doc.GenerateHelp", null);
        java("org.h2.build.i18n.PrepareTranslation", null);
        java("org.h2.build.indexer.Indexer", null);
        java("org.h2.build.doc.MergeDocs", null);
        java("org.h2.build.doc.WebSite", null);
        java("org.h2.build.doc.LinkChecker", null);
        java("org.h2.build.doc.XMLChecker", null);
        java("org.h2.build.doc.SpellChecker", null);
        java("org.h2.build.code.CheckTextFiles", null);
        beep();
    }

    /**
     * Download all required jar files. Actually those are only compile time
     * dependencies. The database can be used without any dependencies.
     */
    public void download() {
        downloadOrVerify(false);
    }

    private void downloadOrVerify(boolean offline) {
        downloadOrVerify("ext/servlet-api-2.4.jar", "javax/servlet", "servlet-api", "2.4",
                "3fc542fe8bb8164e8d3e840fe7403bc0518053c0", offline);
        if (getLuceneVersion() == 3) {
            downloadOrVerify("ext/lucene-core-3.0.2.jar", "org/apache/lucene", "lucene-core", "3.0.2",
                    "c2b48995ab855c1b9ea13867a0f976c994e0105d", offline);
        } else {
            downloadOrVerify("ext/lucene-core-2.2.0.jar", "org/apache/lucene", "lucene-core", "2.2.0",
                    "47b6eee2e17bd68911e7045896a1c09de0b2dda8", offline);
        }
        downloadOrVerify("ext/slf4j-api-1.6.0.jar", "org/slf4j", "slf4j-api", "1.6.0",
                "b353147a7d51fcfcd818d8aa6784839783db0915", offline);
        downloadOrVerify("ext/org.osgi.core-4.2.0.jar", "org/osgi", "org.osgi.core", "4.2.0",
                "66ab449ff3aa5c4adfc82c89025cc983b422eb95", offline);
        downloadOrVerify("ext/org.osgi.enterprise-4.2.0.jar", "org/osgi", "org.osgi.enterprise", "4.2.0",
                "8634dcb0fc62196e820ed0f1062993c377f74972", offline);
        downloadOrVerify("ext/jts-1.13.jar", "com/vividsolutions", "jts", "1.13",
                "3ccfb9b60f04d71add996a666ceb8902904fd805", offline);
    }

    private void downloadOrVerify(String target, String group, String artifact,
            String version, String sha1Checksum, boolean offline) {
        if (offline) {
            File targetFile = new File(target);
            if (targetFile.exists()) {
                return;
            }
            println("Missing file: " + target);
            filesMissing = true;
        } else {
            downloadUsingMaven(target, group, artifact, version, sha1Checksum);
        }
    }

    private void downloadTest() {
        // for TestUpgrade
        download("ext/h2mig_pagestore_addon.jar",
                "http://h2database.com/h2mig_pagestore_addon.jar",
                "6dfafe1b86959c3ba4f7cf03e99535e8b9719965");
        // for TestOldVersion
        downloadUsingMaven("ext/h2-1.2.127.jar", "com/h2database", "h2", "1.2.127",
                "056e784c7cf009483366ab9cd8d21d02fe47031a");
        // for TestPgServer

        downloadUsingMaven("ext/postgresql-8.3-603.jdbc3.jar", "postgresql", "postgresql", "8.3-603.jdbc3",
                "33d531c3c53055ddcbea3d88bfa093466ffef924");
        // for TestTraceSystem
        downloadUsingMaven("ext/slf4j-nop-1.6.0.jar", "org/slf4j", "slf4j-nop", "1.6.0",
                "4da67bb4a6eea5dc273f99c50ad2333eadb46f86");
    }

    private static String getVersion() {
        return getStaticValue("org.h2.engine.Constants", "getVersion");
    }

    private static String getLuceneJar() {
        return "lucene-core-" + (getLuceneVersion() == 2 ? "2.2.0" : "3.0.2") + ".jar";
    }

    private static int getLuceneVersion() {
        // use Lucene 2 for H2 1.2.x, and Lucene 3 for H2 1.3.x.
        String s = new String(readFile(new File("src/main/org/h2/engine/Constants.java")));
        int idx = s.indexOf("VERSION_MINOR") + "VERSION_MINOR".length() + 3;
        int version = Integer.parseInt(s.substring(idx, idx + 1));
        return Integer.parseInt(System.getProperty("lucene", "" + version));
    }

    private static String getJarSuffix() {
        return "-" + getVersion() + ".jar";
    }

    /**
     * Create the h2.zip file and the Windows installer.
     */
    public void installer() {
        delete(files("bin").keep("*.jar"));
        jar();
        docs();
        try {
            exec("soffice", args("-invisible", "macro:///Standard.Module1.H2Pdf"));
            copy("docs", files("../h2web/h2.pdf"), "../h2web");
        } catch (Exception e) {
            print("OpenOffice is not available: " + e);
        }
        delete("docs/html/onePage.html");
        FileList files = files("../h2").keep("../h2/build.*");
        files.addAll(files("../h2/bin").keep("../h2/bin/h2*"));
        files.addAll(files("../h2/docs").exclude("*.jar"));
        files.addAll(files("../h2/service"));
        files.addAll(files("../h2/src"));
        zip("../h2web/h2.zip", files, "../", false, false);
        boolean installer = false;
        try {
            exec("makensis", args("/v2", "src/installer/h2.nsi"));
            installer = true;
        } catch (Exception e) {
            print("NSIS is not available: " + e);
        }
        String buildDate = getStaticField("org.h2.engine.Constants", "BUILD_DATE");
        byte[] data = readFile(new File("../h2web/h2.zip"));
        String sha1Zip = getSHA1(data), sha1Exe = null;
        writeFile(new File("../h2web/h2-" + buildDate + ".zip"), data);
        if (installer) {
            data = readFile(new File("../h2web/h2-setup.exe"));
            sha1Exe = getSHA1(data);
            writeFile(new File("../h2web/h2-setup-" + buildDate + ".exe"), data);
        }
        updateChecksum("../h2web/html/download.html", sha1Zip, sha1Exe);
    }

    private static void updateChecksum(String fileName, String sha1Zip, String sha1Exe) {
        String checksums = new String(readFile(new File(fileName)));
        checksums = replaceAll(checksums, "<!-- sha1Zip -->",
                "(SHA1 checksum: " + sha1Zip + ")");
        if (sha1Exe != null) {
            checksums = replaceAll(checksums, "<!-- sha1Exe -->",
                    "(SHA1 checksum: " + sha1Exe + ")");
        }
        writeFile(new File(fileName), checksums.getBytes());
    }

    /**
     * Create the regular h2.jar file.
     */
    public void jar() {
        compile();
        manifest("H2 Database Engine", "org.h2.tools.Console");
        FileList files = files("temp").
            exclude("temp/android/*").
            exclude("temp/org/h2/android/*").
            exclude("temp/org/h2/build/*").
            exclude("temp/org/h2/dev/*").
            exclude("temp/org/h2/jcr/*").
            exclude("temp/org/h2/jaqu/*").
            exclude("temp/org/h2/java/*").
            exclude("temp/org/h2/jcr/*").
            exclude("temp/org/h2/mode/*").
            exclude("temp/org/h2/samples/*").
            exclude("temp/org/h2/server/ftp/*").
            exclude("temp/org/h2/test/*").
            exclude("*.bat").
            exclude("*.sh").
            exclude("*.txt");
        jar("bin/h2" + getJarSuffix(), files, "temp");
        filter("src/installer/h2.sh", "bin/h2.sh", "h2.jar", "h2" + getJarSuffix());
        filter("src/installer/h2.bat", "bin/h2.bat", "h2.jar", "h2" + getJarSuffix());
        filter("src/installer/h2w.bat", "bin/h2w.bat", "h2.jar", "h2" + getJarSuffix());
    }

    /**
     * Create the file h2android.jar. This only contains the embedded database,
     * plus the H2 Android API. Debug information is disabled.
     */
    public void jarAndroid() {
        compile(false, false, true);
        FileList files = files("temp").
            exclude("temp/org/h2/bnf/*").
            exclude("temp/org/h2/build/*").
            exclude("temp/org/h2/dev/*").
            exclude("temp/org/h2/fulltext/*").
            exclude("temp/org/h2/jaqu/*").
            exclude("temp/org/h2/java/*").
            exclude("temp/org/h2/jdbcx/*").
            exclude("temp/org/h2/jcr/*").
            exclude("temp/org/h2/jmx/*").
            exclude("temp/org/h2/mode/*").
            exclude("temp/org/h2/samples/*").
            exclude("temp/org/h2/server/*").
            exclude("temp/org/h2/test/*").
            exclude("temp/org/h2/tools/*").
            exclude("*.bat").
            exclude("*.sh").
            exclude("*.txt");
        files.add(new File("temp/org/h2/tools/DeleteDbFiles.class"));
        files.add(new File("temp/org/h2/tools/CompressTool.class"));
        jar("bin/h2android" + getJarSuffix(), files, "temp");
    }

    /**
     * Create the h2client.jar. This only contains the remote JDBC
     * implementation.
     */
    public void jarClient() {
        compile(true, true, false);
        FileList files = files("temp").
            exclude("temp/org/h2/build/*").
            exclude("temp/org/h2/dev/*").
            exclude("temp/org/h2/jaqu/*").
            exclude("temp/org/h2/java/*").
            exclude("temp/org/h2/jcr/*").
            exclude("temp/org/h2/mode/*").
            exclude("temp/org/h2/samples/*").
            exclude("temp/org/h2/test/*").
            exclude("*.bat").
            exclude("*.sh").
            exclude("*.txt");
        long kb = jar("bin/h2client" + getJarSuffix(), files, "temp");
        if (kb < 350 || kb > 450) {
            throw new RuntimeException("Expected file size 350 - 450 KB, got: " + kb);
        }
    }

    /**
     * Create the file h2mvstore.jar. This only contains the MVStore.
     */
    public void jarMVStore() {
        compileMVStore(true);
        FileList files = files("temp");
        jar("bin/h2mvstore" + getJarSuffix(), files, "temp");
    }

    /**
     * Create the file h2small.jar. This only contains the embedded database.
     * Debug information is disabled.
     */
    public void jarSmall() {
        compile(false, false, true);
        FileList files = files("temp").
            exclude("temp/android/*").
            exclude("temp/org/h2/android/*").
            exclude("temp/org/h2/build/*").
            exclude("temp/org/h2/dev/*").
            exclude("temp/org/h2/jcr/*").
            exclude("temp/org/h2/jaqu/*").
            exclude("temp/org/h2/java/*").
            exclude("temp/org/h2/jcr/*").
            exclude("temp/org/h2/mode/*").
            exclude("temp/org/h2/samples/*").
            exclude("temp/org/h2/server/ftp/*").
            exclude("temp/org/h2/test/*").
            exclude("temp/org/h2/bnf/*").
            exclude("temp/org/h2/fulltext/*").
            exclude("temp/org/h2/jdbcx/*").
            exclude("temp/org/h2/jmx/*").
            exclude("temp/org/h2/server/*").
            exclude("temp/org/h2/tools/*").
            exclude("*.bat").
            exclude("*.sh").
            exclude("*.txt");
        files.add(new File("temp/org/h2/tools/DeleteDbFiles.class"));
        files.add(new File("temp/org/h2/tools/CompressTool.class"));
        jar("bin/h2small" + getJarSuffix(), files, "temp");
    }

    /**
     * Create the file h2jaqu.jar. This only contains the JaQu (Java Query)
     * implementation. All other jar files do not include JaQu.
     */
    public void jarJaqu() {
        compile(true, false, true);
        manifest("H2 JaQu", "");
        FileList files = files("temp/org/h2/jaqu");
        files.addAll(files("temp/META-INF/MANIFEST.MF"));
        jar("bin/h2jaqu" + getJarSuffix(), files, "temp");
    }

    /**
     * Create the Javadocs of the API (including the JDBC API) and tools.
     */
    public void javadoc() {
        compileTools();
        delete("docs");
        mkdir("docs/javadoc");
        javadoc("-sourcepath", "src/main", "org.h2.jdbc", "org.h2.jdbcx",
                "org.h2.tools", "org.h2.api", "org.h2.constant", "org.h2.fulltext",
                "-classpath",
                "ext/" + getLuceneJar() +
                File.pathSeparator + "ext/jts-1.13.jar",
                "-docletpath", "bin" + File.pathSeparator + "temp",
                "-doclet", "org.h2.build.doclet.Doclet");
        copy("docs/javadoc", files("src/docsrc/javadoc"), "src/docsrc/javadoc");
    }

    /**
     * Create the Javadocs of the implementation.
     */
    public void javadocImpl() {
        compileTools();
        mkdir("docs/javadocImpl2");
        javadoc("-sourcepath", "src/main" + File.pathSeparator +
                "src/test" + File.pathSeparator + "src/tools" ,
                "-noindex",
                "-tag", "h2.resource",
                "-d", "docs/javadocImpl2",
                "-classpath", System.getProperty("java.home") +
                "/../lib/tools.jar" +
                File.pathSeparator + "ext/slf4j-api-1.6.0.jar" +
                File.pathSeparator + "ext/servlet-api-2.4.jar" +
                File.pathSeparator + "ext/" + getLuceneJar() +
                File.pathSeparator + "ext/org.osgi.core-4.2.0.jar" +
                File.pathSeparator + "ext/org.osgi.enterprise-4.2.0.jar" +
                File.pathSeparator + "ext/jts-1.13.jar",
                "-subpackages", "org.h2",
                "-exclude", "org.h2.test.jaqu:org.h2.jaqu");
        System.setProperty("h2.interfacesOnly", "false");
        System.setProperty("h2.javadocDestDir", "docs/javadocImpl");
        javadoc("-sourcepath", "src/main" + File.pathSeparator + "src/test" + File.pathSeparator + "src/tools",
                "-classpath", System.getProperty("java.home") + "/../lib/tools.jar" +
                File.pathSeparator + "ext/slf4j-api-1.6.0.jar" +
                File.pathSeparator + "ext/servlet-api-2.4.jar" +
                File.pathSeparator + "ext/" + getLuceneJar() +
                File.pathSeparator + "ext/org.osgi.core-4.2.0.jar" +
                File.pathSeparator + "ext/org.osgi.enterprise-4.2.0.jar" +
                File.pathSeparator + "ext/jts-1.13.jar",
                "-subpackages", "org.h2",
                "-exclude", "org.h2.test.jaqu:org.h2.jaqu",
                "-package",
                "-docletpath", "bin" + File.pathSeparator + "temp",
                "-doclet", "org.h2.build.doclet.Doclet");
        copy("docs/javadocImpl", files("src/docsrc/javadoc"), "src/docsrc/javadoc");
    }

    private static void manifest(String title, String mainClassName) {
        String manifest = new String(readFile(new File("src/main/META-INF/MANIFEST.MF")));
        manifest = replaceAll(manifest, "${title}", title);
        manifest = replaceAll(manifest, "${version}", getVersion());
        manifest = replaceAll(manifest, "${buildJdk}", getJavaSpecVersion());
        String createdBy = System.getProperty("java.runtime.version") +
            " (" + System.getProperty("java.vm.vendor") + ")";
        manifest = replaceAll(manifest, "${createdBy}", createdBy);
        String mainClassTag = manifest == null ? "" : "Main-Class: " + mainClassName;
        manifest = replaceAll(manifest, "${mainClassTag}", mainClassTag);
        writeFile(new File("temp/META-INF/MANIFEST.MF"), manifest.getBytes());
    }

    /**
     * This will build a release of the H2 .jar file and upload it to
     * file:///data/h2database/m2-repo. This is only required when
     * a new H2 version is made.
     */
    public void mavenDeployCentral() {
        // generate and deploy h2*-sources.jar file
        FileList files = files("src/main");
        copy("docs", files, "src/main");
        files = files("docs").keep("docs/org/*").keep("*.java");
        files.addAll(files("docs").keep("docs/META-INF/*"));
        String manifest = new String(readFile(new File("src/installer/source-manifest.mf")));
        manifest = replaceAll(manifest, "${version}", getVersion());
        writeFile(new File("docs/META-INF/MANIFEST.MF"), manifest.getBytes());
        jar("docs/h2-" + getVersion() + "-sources.jar", files, "docs");
        delete("docs/org");
        delete("docs/META-INF");
        // the option -DgeneratePom=false doesn't work with some versions of
        // Maven because of bug http://jira.codehaus.org/browse/MDEPLOY-84
        // as a workaround we generate the pom, but overwrite it later on
        // (that's why the regular jar is created at the very end)
        execScript("mvn", args(
                "deploy:deploy-file",
                "-Dfile=docs/h2-" + getVersion() + "-sources.jar",
                "-Durl=file:///data/h2database/m2-repo",
                "-Dpackaging=jar",
                "-Dclassifier=sources",
                "-Dversion=" + getVersion(),
                "-DartifactId=h2",
                "-DgroupId=com.h2database"
                // ,"-DgeneratePom=false"
                ));

        // generate and deploy the h2*-javadoc.jar file
        javadocImpl();
        files = files("docs/javadocImpl2");
        jar("docs/h2-" + getVersion() + "-javadoc.jar", files, "docs/javadocImpl2");
        execScript("mvn", args(
                "deploy:deploy-file",
                "-Dfile=docs/h2-" + getVersion() + "-javadoc.jar",
                "-Durl=file:///data/h2database/m2-repo",
                "-Dpackaging=jar",
                "-Dclassifier=javadoc",
                "-Dversion=" + getVersion(),
                "-DartifactId=h2",
                "-DgroupId=com.h2database"
                // ,"-DgeneratePom=false"
                ));

        // generate and deploy the h2*.jar file
        jar();
        String pom = new String(readFile(new File("src/installer/pom-template.xml")));
        pom = replaceAll(pom, "@version@", getVersion());
        writeFile(new File("bin/pom.xml"), pom.getBytes());
        execScript("mvn", args(
                "deploy:deploy-file",
                "-Dfile=bin/h2" + getJarSuffix(),
                "-Durl=file:///data/h2database/m2-repo",
                "-Dpackaging=jar",
                "-Dversion=" + getVersion(),
                "-DpomFile=bin/pom.xml",
                "-DartifactId=h2",
                "-DgroupId=com.h2database"));
    }

    /**
     * This will build a 'snapshot' H2 .jar file and upload it the to the local
     * Maven 2 repository.
     */
    public void mavenInstallLocal() {
        jar();
        String pom = new String(readFile(new File("src/installer/pom-template.xml")));
        pom = replaceAll(pom, "@version@", "1.0-SNAPSHOT");
        writeFile(new File("bin/pom.xml"), pom.getBytes());
        execScript("mvn", args(
                "install:install-file",
                "-Dversion=1.0-SNAPSHOT",
                "-Dfile=bin/h2" + getJarSuffix(),
                "-Dpackaging=jar",
                "-DpomFile=bin/pom.xml",
                "-DartifactId=h2",
                "-DgroupId=com.h2database"));
    }

    /**
     * Build the jar file without downloading any files over the network. If the
     * required files are missing, they are are listed, and the jar file is not
     * built.
     */
    public void offline() {
        downloadOrVerify(true);
        if (filesMissing) {
            println("Required files are missing");
            println("Both Lucene 2 and 3 are supported using -Dlucene=x (x=2 or 3)");
        } else {
            jar();
        }
    }

    private void resources(boolean clientOnly, boolean basicOnly) {
        if (!clientOnly) {
            java("org.h2.build.doc.GenerateHelp", null);
            javadoc("-sourcepath", "src/main", "org.h2.tools", "org.h2.jmx",
                    "-classpath",
                    "ext/" + getLuceneJar() +
                    File.pathSeparator + "ext/jts-1.13.jar",
                    "-docletpath", "bin" + File.pathSeparator + "temp",
                    "-doclet", "org.h2.build.doclet.ResourceDoclet");
        }
        FileList files = files("src/main").
            exclude("*.MF").
            exclude("*.java").
            exclude("*/package.html").
            exclude("*/java.sql.Driver");
        if (basicOnly) {
            files = files.keep("src/main/org/h2/res/_messages_en.*");
        }
        if (clientOnly) {
            files = files.exclude("src/main/org/h2/res/javadoc.properties");
            files = files.exclude("src/main/org/h2/server/*");
        }
        zip("temp/org/h2/util/data.zip", files, "src/main", true, false);
    }

    /**
     * Just run the spellchecker.
     */
    public void spellcheck() {
        java("org.h2.build.doc.SpellChecker", null);
    }

    /**
     * Compile and run all tests. This does not include the compile step.
     */
    public void test() {
        downloadTest();
        String testClass = System.getProperty("test", "org.h2.test.TestAll");
        java(testClass, null);
    }

    /**
     * Print the system properties
     */
    public void testSysProperties() {
        System.out.println("environment settings:");
        for (Entry<Object, Object> e : new TreeMap<Object, Object>(System.getProperties()).entrySet()) {
            System.out.println(e);
        }
    }

    /**
     * Test the local network of this machine.
     */
    public void testNetwork() {
        try {
            long start = System.currentTimeMillis();
            System.out.println("localhost:");
            System.out.println("  " + InetAddress.getByName("localhost"));
            for (InetAddress address : InetAddress.getAllByName("localhost")) {
                System.out.println("  " + address);
            }
            InetAddress localhost = InetAddress.getLocalHost();
            System.out.println("getLocalHost:" + localhost);
            for (InetAddress address : InetAddress.getAllByName(localhost.getHostAddress())) {
                System.out.println("  " + address);
            }
            InetAddress address = InetAddress.getByName(localhost.getHostAddress());
            System.out.println("byName:" + address);
            ServerSocket serverSocket;
            try {
                serverSocket = new ServerSocket(0);
            } catch (Exception e) {
                e.printStackTrace();
                serverSocket = new ServerSocket(0);
            }
            System.out.println(serverSocket);
            int port = serverSocket.getLocalPort();
            final ServerSocket accept = serverSocket;
            start = System.currentTimeMillis();
            Thread thread = new Thread() {
                @Override
                public void run() {
                    try {
                        System.out.println("server accepting");
                        Socket s = accept.accept();
                        Thread.sleep(100);
                        System.out.println("server accepted:" + s);
                        System.out.println("server read:" + s.getInputStream().read());
                        Thread.sleep(200);
                        s.getOutputStream().write(234);
                        Thread.sleep(100);
                        System.out.println("server closing");
                        s.close();
                        System.out.println("server done");
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            };
            thread.start();
            System.out.println("time: " + (System.currentTimeMillis() - start));
            Thread.sleep(1000);
            start = System.currentTimeMillis();
            final Socket socket = new Socket();
            socket.setSoTimeout(2000);
            final InetSocketAddress socketAddress = new InetSocketAddress(address, port);
            System.out.println("client:" + socketAddress);
            try {
                Thread t = new Thread() {
                    @Override
                    public void run() {
                        try {
                            socket.connect(socketAddress, 2000);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                };
                t.start();
                t.join(5000);
                if (!socket.isConnected()) {
                    final InetSocketAddress localhostAddress = new InetSocketAddress("localhost", port);
                    System.out.println("not connected, trying localhost:" + socketAddress);
                    socket.connect(localhostAddress, 2000);
                }
                System.out.println("time: " + (System.currentTimeMillis() - start));
                Thread.sleep(200);
                start = System.currentTimeMillis();
                System.out.println("client:" + socket.toString());
                socket.getOutputStream().write(123);
                System.out.println("time: " + (System.currentTimeMillis() - start));
                Thread.sleep(100);
                start = System.currentTimeMillis();
                System.out.println("client read:" + socket.getInputStream().read());
                socket.close();
            } catch (Throwable t) {
                t.printStackTrace();
            }
            thread.join(5000);
            System.out.println("time: " + (System.currentTimeMillis() - start));
            if (thread.isAlive()) {
                System.out.println("thread is still alive, interrupting");
                thread.interrupt();
            }
            Thread.sleep(100);
            System.out.println("done");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * This build target is used for the automated build. It copies the result
     * of the automated build (including test results, newsfeed, code coverage)
     * to the public web site.
     */
    public void uploadBuild() {
        String password = System.getProperty("h2.ftpPassword");
        if (password == null) {
            throw new RuntimeException("h2.ftpPassword not set");
        }
        downloadTest();
        FileList files = files("src/tools").keep("*/UploadBuild.java");
        StringList args = args("-d", "temp", "-sourcepath", "src/tools" +
                File.pathSeparator + "src/test" + File.pathSeparator + "src/main");
        mkdir("temp");
        javac(args, files);
        String cp = "bin" + File.pathSeparator + "temp" +
                File.pathSeparator + "ext/h2mig_pagestore_addon.jar";
        exec("java", args("-Xmx512m", "-cp", cp,
                "-Dh2.ftpPassword=" + password,
                "org.h2.build.doc.UploadBuild"));
    }

    /**
     * Build the h2console.war file.
     */
    public void warConsole() {
        jar();
        copy("temp/WEB-INF", files("src/tools/WEB-INF/web.xml"), "src/tools/WEB-INF");
        copy("temp", files("src/tools/WEB-INF/console.html"), "src/tools/WEB-INF");
        copy("temp/WEB-INF/lib", files("bin/h2" + getJarSuffix()), "bin");
        FileList files = files("temp").exclude("temp/org*").exclude("temp/META-INF*");
        jar("bin/h2console.war", files, "temp");
    }

    @Override
    protected String getLocalMavenDir() {
        String userHome = System.getProperty("user.home", "");
        File file = new File(userHome, ".m2/settings.xml");
        if (!file.exists()) {
            return super.getLocalMavenDir();
        }
        XMLParser p = new XMLParser(new String(BuildBase.readFile(file)));
        HashMap<String, String> prop = new HashMap<String, String>();
        for (String name = ""; p.hasNext();) {
            int event = p.next();
            if (event == XMLParser.START_ELEMENT) {
                name += "/" + p.getName();
            } else if (event == XMLParser.END_ELEMENT) {
                name = name.substring(0, name.lastIndexOf('/'));
            } else if (event == XMLParser.CHARACTERS) {
                String text = p.getText().trim();
                if (text.length() > 0) {
                    prop.put(name, text);
                }
            }
        }
        String local = prop.get("/settings/localRepository");
        if (local == null) {
            local = "${user.home}/.m2/repository";
        }
        local = replaceAll(local, "${user.home}", userHome);
        return local;
    }

}
