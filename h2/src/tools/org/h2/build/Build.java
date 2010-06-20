/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
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
import java.util.TreeMap;
import java.util.Map.Entry;

import org.h2.build.code.SwitchSource;

/**
 * The build definition.
 */
public class Build extends BuildBase {

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
        compile();

        // this will download an old version of HSQLDB;
        // to use a more recent version, download it manually
        // and add it to the 'ext' directory as 'hsqldb.jar'.
        download("ext/hsqldb-1.8.0.10.jar",
                "http://repo1.maven.org/maven2/hsqldb/hsqldb/1.8.0.10/hsqldb-1.8.0.10.jar",
                "7e9978fdb754bce5fcd5161133e7734ecb683036");

        download("ext/derby-10.6.1.0.jar",
                "http://repo1.maven.org/maven2/org/apache/derby/derby/10.6.1.0/derby-10.6.1.0.jar",
                "01137cd636b0e3c22f0d273478adb58aa30e984a");
        download("ext/derbyclient-10.6.1.0.jar",
                "http://repo1.maven.org/maven2/org/apache/derby/derbyclient/10.6.1.0/derbyclient-10.6.1.0.jar",
                "e7c6fbaca2ef4dbcad27fa7d8a9cd1ac0d1e4b00");
        download("ext/derbynet-10.6.1.0.jar",
                "http://repo1.maven.org/maven2/org/apache/derby/derbynet/10.6.1.0/derbynet-10.6.1.0.jar",
                "d5d9d7b783eeaef016be85c34d5c65d1e7cec764");
        download("ext/postgresql-8.3-603.jdbc3.jar",
                "http://repo1.maven.org/maven2/postgresql/postgresql/8.3-603.jdbc3/postgresql-8.3-603.jdbc3.jar",
                "33d531c3c53055ddcbea3d88bfa093466ffef924");
        download("ext/mysql-connector-java-5.1.6.jar",
                "http://repo1.maven.org/maven2/mysql/mysql-connector-java/5.1.6/mysql-connector-java-5.1.6.jar",
                "380ef5226de2c85ff3b38cbfefeea881c5fce09d");
        String cp = "temp" + File.pathSeparator + "bin/h2" + getJarSuffix() + File.pathSeparator +
        "ext/hsqldb.jar" + File.pathSeparator +
        "ext/hsqldb-1.8.0.10.jar" + File.pathSeparator +
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

    /**
     * Run the Emma code coverage.
     */
    public void coverage() {
        downloadTest();
        download("ext/emma-2.0.5312.jar",
                "http://repo2.maven.org/maven2/emma/emma/2.0.5312/emma-2.0.5312.jar",
                "30a40933caf67d88d9e75957950ccf353b181ab7");
        String cp = "temp" + File.pathSeparator + "bin" +
            File.pathSeparator + "ext/emma-2.0.5312.jar" +
            File.pathSeparator + "ext/postgresql-8.3-603.jdbc3.jar" +
            File.pathSeparator + "ext/servlet-api-2.4.jar" +
            File.pathSeparator + "ext/lucene-core-2.2.0.jar" +
            File.pathSeparator + "ext/org.osgi.core-1.2.0.jar" +
            File.pathSeparator + "ext/slf4j-api-1.5.0.jar";
        exec("java", args("-Xmx128m", "-cp", cp, "emma", "run",
                "-cp", "temp",
                "-sp", "src/main",
                "-r", "html,txt",
                "-ix", "-org.h2.test.*,-org.h2.dev.*,-org.h2.jaqu.*,-org.h2.mode.*",
                "org.h2.test.TestAll"));
    }

    /**
     * Switch the source code to the current JDK.
     */
    public void switchSource() {
        try {
            String version = System.getProperty("version");
            if (version == null) {
                SwitchSource.main("-dir", "src", "-auto");
            } else {
                SwitchSource.main("-dir", "src", "-version", version);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void compile(boolean debugInfo, boolean clientOnly, boolean basicResourcesOnly) {
        switchSource();
        clean();
        mkdir("temp");
        download();
        String classpath = "temp" +
                File.pathSeparator + "ext/servlet-api-2.4.jar" +
                File.pathSeparator + "ext/lucene-core-2.2.0.jar" +
                File.pathSeparator + "ext/slf4j-api-1.5.0.jar" +
                File.pathSeparator + "ext/org.osgi.core-1.2.0.jar" +
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
            args = args.plus("-d", "temp", "-sourcepath", "src/main", "-classpath", classpath);
        } else {
            args = args.plus("-g:none", "-d", "temp", "-sourcepath", "src/main", "-classpath", classpath);
        }
        javac(args, files);

        files = files("src/main/META-INF/services");
        copy("temp", files, "src/main");

        if (!clientOnly) {
            files = files("src/test");
            files.addAll(files("src/tools"));
            args = args("-d", "temp", "-sourcepath", "src/test" + File.pathSeparator + "src/tools",
                    "-classpath", classpath);
            javac(args, files);
            files = files("src/test").
                exclude("*.java").
                exclude("*/package.html");
            copy("temp", files, "src/test");
        }
        resources(clientOnly, basicResourcesOnly);
    }

    private void filter(String source, String target, String old, String replacement) {
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
        download("ext/servlet-api-2.4.jar",
                "http://repo1.maven.org/maven2/javax/servlet/servlet-api/2.4/servlet-api-2.4.jar",
                "3fc542fe8bb8164e8d3e840fe7403bc0518053c0");
        download("ext/lucene-core-2.2.0.jar",
                "http://repo1.maven.org/maven2/org/apache/lucene/lucene-core/2.2.0/lucene-core-2.2.0.jar",
                "47b6eee2e17bd68911e7045896a1c09de0b2dda8");
        download("ext/slf4j-api-1.5.0.jar",
                "http://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.5.0/slf4j-api-1.5.0.jar",
                "b2df265d02350ecfe87b6c1773c7c4fab2b33505");
        download("ext/org.osgi.core-1.2.0.jar",
                "http://repo1.maven.org/maven2/org/apache/felix/org.osgi.core/1.2.0/org.osgi.core-1.2.0.jar",
                "3006beb1ca6a83449def6127dad3c060148a0209");
    }

    private void downloadTest() {
        // for TestOldVersion
        download("ext/h2-1.2.127.jar",
                "http://repo1.maven.org/maven2/com/h2database/h2/1.2.127/h2-1.2.127.jar",
                "056e784c7cf009483366ab9cd8d21d02fe47031a");
    }

    private String getVersion() {
        return getStaticValue("org.h2.engine.Constants", "getVersion");
    }

    private String getJarSuffix() {
        return "-" + getVersion() + ".jar";
    }

    /**
     * Create the h2.zip file and the Windows installer.
     */
    public void installer() {
        delete(files("bin").keep("*.jar"));
        jar();
        docs();
        exec("soffice", args("-invisible", "macro:///Standard.Module1.H2Pdf"));
        copy("docs", files("../h2web/h2.pdf"), "../h2web");
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

    private void updateChecksum(String fileName, String sha1Zip, String sha1Exe) {
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
            exclude("temp/org/h2/build/*").
            exclude("temp/org/h2/dev/*").
            exclude("temp/org/h2/jcr/*").
            exclude("temp/org/h2/jaqu/*").
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
     * Create the h2client.jar. This only contains the remote JDBC
     * implementation.
     */
    public void jarClient() {
        compile(true, true, false);
        FileList files = files("temp").
            exclude("temp/org/h2/build/*").
            exclude("temp/org/h2/dev/*").
            exclude("temp/org/h2/jaqu/*").
            exclude("temp/org/h2/mode/*").
            exclude("temp/org/h2/samples/*").
            exclude("temp/org/h2/test/*").
            exclude("*.bat").
            exclude("*.sh").
            exclude("*.txt");
        long kb = jar("bin/h2client" + getJarSuffix(), files, "temp");
        if (kb < 300 || kb > 400) {
            throw new RuntimeException("Expected file size 300 - 400 KB, got: " + kb);
        }
    }

    /**
     * Create the file h2small.jar. This only contains the embedded database.
     * Debug information is disabled.
     */
    public void jarSmall() {
        compile(false, false, true);
        FileList files = files("temp").
            exclude("temp/org/h2/bnf/*").
            exclude("temp/org/h2/build/*").
            exclude("temp/org/h2/dev/*").
            exclude("temp/org/h2/fulltext/*").
            exclude("temp/org/h2/jaqu/*").
            exclude("temp/org/h2/jdbcx/*").
            exclude("temp/org/h2/mode/*").
            exclude("temp/org/h2/samples/*").
            exclude("temp/org/h2/server/*").
            exclude("temp/org/h2/test/*").
            exclude("temp/org/h2/tools/*").
            exclude("*.bat").
            exclude("*.sh").
            exclude("*.txt");
        files.add(new File("temp/org/h2/tools/DeleteDbFiles.class"));
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
        delete("docs");
        mkdir("docs/javadoc");
        javadoc("-sourcepath", "src/main", "org.h2.jdbc", "org.h2.jdbcx",
                "org.h2.tools", "org.h2.api", "org.h2.constant", "org.h2.fulltext",
                "-classpath", "ext/lucene-core-2.2.0.jar",
                "-docletpath", "bin" + File.pathSeparator + "temp",
                "-doclet", "org.h2.build.doclet.Doclet");
        copy("docs/javadoc", files("src/docsrc/javadoc"), "src/docsrc/javadoc");
    }

    /**
     * Create the Javadocs of the implementation.
     */
    public void javadocImpl() {
        mkdir("docs/javadocImpl2");
        javadoc("-sourcepath", "src/main" + File.pathSeparator +
                "src/test" + File.pathSeparator + "src/tools" ,
                "-noindex",
                "-tag", "h2.resource",
                "-d", "docs/javadocImpl2",
                "-classpath", System.getProperty("java.home") +
                "/../lib/tools.jar" +
                File.pathSeparator + "ext/slf4j-api-1.5.0.jar" +
                File.pathSeparator + "ext/servlet-api-2.4.jar" +
                File.pathSeparator + "ext/lucene-core-2.2.0.jar" +
                File.pathSeparator + "ext/org.osgi.core-1.2.0.jar",
                "-subpackages", "org.h2",
                "-exclude", "org.h2.test.jaqu:org.h2.jaqu");
        System.setProperty("h2.interfacesOnly", "false");
        System.setProperty("h2.javadocDestDir", "docs/javadocImpl");
        javadoc("-sourcepath", "src/main" + File.pathSeparator + "src/test" + File.pathSeparator + "src/tools",
                "-classpath", System.getProperty("java.home") + "/../lib/tools.jar" +
                File.pathSeparator + "ext/slf4j-api-1.5.0.jar" +
                File.pathSeparator + "ext/servlet-api-2.4.jar" +
                File.pathSeparator + "ext/lucene-core-2.2.0.jar" +
                File.pathSeparator + "ext/org.osgi.core-1.2.0.jar",
                "-subpackages", "org.h2",
                "-exclude", "org.h2.test.jaqu:org.h2.jaqu",
                "-package",
                "-docletpath", "bin" + File.pathSeparator + "temp",
                "-doclet", "org.h2.build.doclet.Doclet");
        copy("docs/javadocImpl", files("src/docsrc/javadoc"), "src/docsrc/javadoc");
    }

    private void manifest(String title, String mainClassName) {
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
        FileList files = files("src/main").keep("*.java");
        jar("docs/h2-" + getVersion() + "-sources.jar", files, "src/main");
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

    private void resources(boolean clientOnly, boolean basicOnly) {
        if (!clientOnly) {
            java("org.h2.build.doc.GenerateHelp", null);
            javadoc("-sourcepath", "src/main", "org.h2.tools",
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
     * Compile and run all tests.
     */
    public void test() {
        downloadTest();
        compile();
        String testClass = System.getProperty("test", "org.h2.test.TestAll");
        java(testClass, null);
    }

    /**
     * Test the local network of this machine.
     */
    public void testNetwork() {
        try {
            System.out.println("environment settings:");
            for (Entry<Object, Object> e : new TreeMap<Object, Object>(System.getProperties()).entrySet()) {
                System.out.println(e);
            }
            System.out.println();
            System.out.println("localhost:" + InetAddress.getByName("localhost"));
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
            new Thread() {
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
            }.start();
            Thread.sleep(1000);
            Socket socket = new Socket();
            InetSocketAddress socketAddress = new InetSocketAddress(address, port);
            System.out.println("client:" + socketAddress);
            try {
                socket.connect(socketAddress, 2000);
                Thread.sleep(200);
                System.out.println("client:" + socket.toString());
                socket.getOutputStream().write(123);
                Thread.sleep(100);
                System.out.println("client read:" + socket.getInputStream().read());
                socket.close();
            } catch (Throwable t) {
                t.printStackTrace();
            }
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
        String cp = "bin" + File.pathSeparator + "temp";
        exec("java", args("-Xmx128m", "-cp", cp,
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

}
