/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build;

import java.io.File;
import java.io.IOException;

import org.h2.build.code.SwitchSource;

/**
 * The build definition.
 */
public class Build extends BuildBase {

    /**
     * Run the build.
     * 
     * @params args the command line arguments
     */
    public static void main(String[] args) {
        new Build().run(args);
    }

    public void all() {
        jar();
        docs();
    }
    
    public void benchmark() {
        download("ext/hsqldb-1.8.0.7.jar",
                "http://repo1.maven.org/maven2/hsqldb/hsqldb/1.8.0.7/hsqldb-1.8.0.7.jar",
                "20554954120b3cc9f08804524ec90113a73f3015");
        download("ext/derby-10.4.1.3.jar",
                "http://repo1.maven.org/maven2/org/apache/derby/derby/10.4.1.3/derby-10.4.1.3.jar",
                "01c19aaea2e971203f410c5263214a890f340342");
        download("ext/derbyclient-10.4.1.3.jar",
                "http://repo1.maven.org/maven2/org/apache/derby/derbyclient/10.4.1.3/derbyclient-10.4.1.3.jar",
                "14e97efccae4d622dfcc65bf1f042588c7bd4cb3");
        download("ext/derbynet-10.4.1.3.jar",
                "http://repo1.maven.org/maven2/org/apache/derby/derbynet/10.4.1.3/derbynet-10.4.1.3.jar",
                "cdf87901e6dfa25c7bc7fa509731bbca04b46191");
        download("ext/postgresql-8.3-603.jdbc3.jar",
                "http://repo1.maven.org/maven2/postgresql/postgresql/8.3-603.jdbc3/postgresql-8.3-603.jdbc3.jar", 
                "33d531c3c53055ddcbea3d88bfa093466ffef924");
        download("ext/mysql-connector-java-5.1.6.jar",
                "http://repo1.maven.org/maven2/mysql/mysql-connector-java/5.1.6/mysql-connector-java-5.1.6.jar", 
                "380ef5226de2c85ff3b38cbfefeea881c5fce09d");
        String cp = "bin" + File.pathSeparator + "bin/h2.jar" + File.pathSeparator +
        "ext/hsqldb-1.8.0.7.jar" + File.pathSeparator +
        "ext/derby-10.4.1.3.jar" + File.pathSeparator +
        "ext/derbyclient-10.4.1.3.jar" + File.pathSeparator +
        "ext/derbynet-10.4.1.3.jar" + File.pathSeparator +
        "ext/postgresql-8.3-603.jdbc3.jar" + File.pathSeparator +
        "ext/mysql-connector-java-5.1.6.jar";
        exec("java", new String[]{"-Xmx128m", "-cp", cp, "org.h2.test.bench.TestPerformance", "-init", "-db", "1"});
        exec("java", new String[]{"-Xmx128m", "-cp", cp, "org.h2.test.bench.TestPerformance", "-db", "2"});
        exec("java", new String[]{"-Xmx128m", "-cp", cp, "org.h2.test.bench.TestPerformance", "-db", "3", "-out", "pe.html"});
        exec("java", new String[]{"-Xmx128m", "-cp", cp, "org.h2.test.bench.TestPerformance", "-init", "-db", "4"});
        exec("java", new String[]{"-Xmx128m", "-cp", cp, "org.h2.test.bench.TestPerformance", "-db", "5"});
        exec("java", new String[]{"-Xmx128m", "-cp", cp, "org.h2.test.bench.TestPerformance", "-db", "6"});
        exec("java", new String[]{"-Xmx128m", "-cp", cp, "org.h2.test.bench.TestPerformance", "-db", "7"});
        exec("java", new String[]{"-Xmx128m", "-cp", cp, "org.h2.test.bench.TestPerformance", "-db", "8", "-out", "ps.html"});
    }
    
    public void clean() {
        delete("temp");
        delete("docs");
        mkdir("docs");
        mkdir("bin");
    }
    
    public void compile() {
        compile(true, false);
    }
    
    private void compile(boolean debugInfo, boolean clientOnly) {
        try {
            SwitchSource.main(new String[] { "-dir", "src", "-auto" });
        } catch (IOException e) {
            throw new Error(e);
        }
        clean();
        mkdir("temp");
        resources(clientOnly);
        download();
        String classpath = "temp" + 
                File.pathSeparator + "ext/servlet-api-2.4.jar" + 
                File.pathSeparator + "ext/lucene-core-2.2.0.jar" + 
                File.pathSeparator + "ext/slf4j-api-1.5.0.jar" + 
                File.pathSeparator + System.getProperty("java.home") + "/../lib/tools.jar";
        FileList files;
        if (clientOnly) {
            files = getFiles("src/main/org/h2/Driver.java");
            files.addAll(getFiles("src/main/org/h2/jdbc"));
            files.addAll(getFiles("src/main/org/h2/jdbcx"));
        } else {
            files = getFiles("src/main");
        }
        if (debugInfo) {
            javac(new String[] { "-d", "temp", "-sourcepath", "src/main", "-classpath", classpath }, files);
        } else {
            javac(new String[] { "-g:none", "-d", "temp", "-sourcepath", "src/main", "-classpath", classpath }, files);
        }
        
        files = getFiles("src/main/META-INF/services");
        copy("temp", files, "src/main");

        if (!clientOnly) {
            files = getFiles("src/test");
            files.addAll(getFiles("src/tools"));
            javac(new String[] { "-d", "temp", "-sourcepath", "src/test" + File.pathSeparator + "src/tools",
                    "-classpath", classpath }, files);

            files = getFiles("src/installer").keep("*.bat");
            files.addAll(getFiles("src/installer").keep("*.sh"));
            copy("bin", files, "src/installer");
    
            files = getFiles("src/test").
                exclude("*.java").
                exclude("*/package.html");
            copy("temp", files, "src/test");
        }
    }

    public void docs() {
        javadoc();
        copy("docs", getFiles("src/docsrc/index.html"), "src/docsrc");
        java("org.h2.build.code.CheckJavadoc", null);
        java("org.h2.build.code.CheckTextFiles", null);
        java("org.h2.build.doc.GenerateDoc", null);
        java("org.h2.build.i18n.PrepareTranslation", null);
        java("org.h2.build.indexer.Indexer", null);
        java("org.h2.build.doc.MergeDocs", null);
        java("org.h2.build.doc.WebSite", null);
        java("org.h2.build.doc.LinkChecker", null);
        java("org.h2.build.doc.XMLChecker", null);
        java("org.h2.build.doc.SpellChecker", null);
    }

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
    }
    
    private String getVersion() {
        return getStaticValue("org.h2.engine.Constants", "getVersion");
    }
    
    public void jar() {
        compile();
        manifest("org.h2.tools.Console");
        FileList files = getFiles("temp").
            exclude("temp/org/h2/dev/*").
            exclude("temp/org/h2/build/*").
            exclude("temp/org/h2/samples/*").
            exclude("temp/org/h2/test/*").
            exclude("*.bat").
            exclude("*.sh").
            exclude("*.txt");
        jar("bin/h2.jar", files, "temp");
    }
    
    public void jarClient() {
        compile(true, true);
        FileList files = getFiles("temp").
            exclude("temp/org/h2/dev/*").
            exclude("temp/org/h2/build/*").
            exclude("temp/org/h2/samples/*").
            exclude("temp/org/h2/test/*").
            exclude("*.bat").
            exclude("*.sh").
            exclude("*.txt");
        jar("bin/h2client.jar", files, "temp");
    }
    
    public void jarSmall() {
        compile(false, false);
        FileList files = getFiles("temp").
            exclude("temp/org/h2/dev/*").
            exclude("temp/org/h2/build/*").
            exclude("temp/org/h2/samples/*").
            exclude("temp/org/h2/test/*").
            exclude("*.bat").
            exclude("*.sh").
            exclude("*.txt").
            exclude("temp/META-INF/*");
        zip("temp/h2classes.zip", files, "temp", true, true);
        manifest("org.h2.tools.Console\nClass-Path: h2classes.zip");
        files = getFiles("temp/h2classes.zip");
        files.addAll(getFiles("temp/META-INF"));
        jar("bin/h2small.jar", files, "temp");
    }
    
    public void javadoc() {
        delete("docs");
        mkdir("docs/javadoc");
        javadoc(new String[] { "-sourcepath", "src/main", "org.h2.jdbc", "org.h2.jdbcx", 
                "org.h2.tools", "org.h2.api", "org.h2.constant", 
                "-doclet", "org.h2.build.doclet.Doclet" });
        copy("docs/javadoc", getFiles("src/docsrc/javadoc"), "src/docsrc/javadoc");
    }        
    
    public void javadocImpl() {
        mkdir("docs/javadocImpl");
        javadoc(new String[] {
                "-sourcepath", "src/main" + File.pathSeparator + "src/test" + File.pathSeparator + "src/tools" , 
                "-noindex",
                "-d", "docs/javadocImpl2",
                "-classpath", System.getProperty("java.home") + "/../lib/tools.jar" + 
                File.pathSeparator + "ext/slf4j-api-1.5.0.jar" +
                File.pathSeparator + "ext/servlet-api-2.4.jar" + 
                File.pathSeparator + "ext/lucene-core-2.2.0.jar",
                "-subpackages", "org.h2",
                "-exclude", "org.h2.build.*,org.h2.dev.*" });
        System.setProperty("h2.interfacesOnly", "false");
        System.setProperty("h2.destDir", "docs/javadocImpl");
        javadoc(new String[] { 
                "-sourcepath", "src/main" + File.pathSeparator + "src/test" + File.pathSeparator + "src/tools", 
                "-classpath", System.getProperty("java.home") + "/../lib/tools.jar" +
                File.pathSeparator + "ext/slf4j-api-1.5.0.jar" +
                File.pathSeparator + "ext/servlet-api-2.4.jar" + 
                File.pathSeparator + "ext/lucene-core-2.2.0.jar",
                "-subpackages", "org.h2",
                "-doclet", "org.h2.build.doclet.Doclet" });
        copy("docs/javadocImpl", getFiles("src/docsrc/javadoc"), "src/docsrc/javadoc");
    }
    
    private void manifest(String mainClassName) {
        String manifest = new String(readFile(new File("src/main/META-INF/MANIFEST.MF")));
        manifest = replaceAll(manifest, "${version}", getVersion());
        manifest = replaceAll(manifest, "${buildJdk}", getJavaSpecVersion());
        String createdBy = System.getProperty("java.runtime.version") + 
            " (" + System.getProperty("java.vm.vendor") + ")";
        manifest = replaceAll(manifest, "${createdBy}", createdBy);
        String mainClassTag = manifest == null ? "" : "Main-Class: " + mainClassName;
        manifest = replaceAll(manifest, "${mainClassTag}", mainClassTag);
        writeFile(new File("temp/META-INF/MANIFEST.MF"), manifest.getBytes());
    }
    
    public void mavenDeployCentral() {
        jar();
        String pom = new String(readFile(new File("src/installer/pom.xml")));
        pom = replaceAll(pom, "@version@", getVersion());
        writeFile(new File("bin/pom.xml"), pom.getBytes());
        execScript("mvn", new String[] { 
                "deploy:deploy-file", 
                "-Dfile=bin/h2.jar",
                "-Durl=file:///data/h2database/m2-repo", 
                "-Dpackaging=jar", 
                "-Dversion=" + getVersion(),
                "-DpomFile=bin/pom.xml", 
                "-DartifactId=h2", 
                "-DgroupId=com.h2database" });
    }

    public void mavenInstallLocal() {
        jar();
        String pom = new String(readFile(new File("src/installer/pom.xml")));
        pom = replaceAll(pom, "@version@", "1.0-SNAPSHOT");
        writeFile(new File("bin/pom.xml"), pom.getBytes());
        execScript("mvn", new String[] { 
                "install:install-file", 
                "-Dversion=1.0-SNAPSHOT", 
                "-Dfile=bin/h2.jar",
                "-Dpackaging=jar", 
                "-DpomFile=bin/pom.xml", 
                "-DartifactId=h2", 
                "-DgroupId=com.h2database" });
    }
    
    private void resources(boolean clientOnly) {
        FileList files = getFiles("src/main").
            exclude("*.java").
            exclude("*/package.html").
            exclude("*/java.sql.Driver");
        if (clientOnly) {
            files = files.exclude("src/main/org/h2/server/*");
        }
        zip("temp/org/h2/util/data.zip", files, "src/main", true, false);
    }
    
    public void spellcheck() {
        java("org.h2.build.doc.SpellChecker", null);
    }
    
    public void zip() {
        FileList files = getFiles("../h2").keep("../h2/build.*");
        files.addAll(getFiles("../h2/bin").keep("../h2/bin/h2.*"));
        files.addAll(getFiles("../h2/docs"));
        files.addAll(getFiles("../h2/service"));
        files.addAll(getFiles("../h2/src"));
        zip("../h2.zip", files, "../", false, false);
    }
    
}
