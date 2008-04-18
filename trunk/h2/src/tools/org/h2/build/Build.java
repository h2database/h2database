/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (license2)
 * Initial Developer: H2 Group
 */
package org.h2.build;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.h2.build.code.SwitchSource;

/**
 * The implementation of the pure Java build.
 */
public class Build extends BuildBase {

    public static void main(String[] args) {
        new Build().run(args);
    }

    public void all() {
        jarSmall();
        docs();
    }
    
    public void spellcheck() {
        java("org.h2.build.doc.SpellChecker", null);
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

    public void javadoc() {
        delete("docs");
        mkdir("docs/javadoc");
        javadoc(new String[] { "-sourcepath", "src/main", "org.h2.jdbc", "org.h2.jdbcx", 
                "org.h2.tools", "org.h2.api", "org.h2.constant", 
                "-doclet", "org.h2.build.doclet.Doclet" });
        copy("docs/javadoc", getFiles("src/docsrc/javadoc"), "src/docsrc/javadoc");
    }        

    public void resources() {
        List files = getFiles("src/main");
        files = filterFiles(files, false, "*.java");
        files = filterFiles(files, false, "*/package.html");
        files = filterFiles(files, false, "*/java.sql.Driver");
        zip("temp/org/h2/util/data.zip", "src/main", files, true, false);
    }

    private void manifest(String mainClassName) {
        String manifest = new String(readFile(new File("src/main/META-INF/MANIFEST.MF")));
        String version = getStaticField("org.h2.engine.Constants", "VERSION");
        manifest = replaceAll(manifest, "${version}", version);
        manifest = replaceAll(manifest, "${buildJdk}", getJavaSpecVersion());
        String createdBy = System.getProperty("java.runtime.version") + " (" + System.getProperty("java.vm.vendor")
                + ")";
        manifest = replaceAll(manifest, "${createdBy}", createdBy);
        String mainClassTag = manifest == null ? "" : "Main-Class: " + mainClassName;
        manifest = replaceAll(manifest, "${mainClassTag}", mainClassTag);
        writeFile(new File("temp/META-INF/MANIFEST.MF"), manifest.getBytes());
    }

    public void jar() {
        compile();
        manifest("org.h2.tools.Console");
        List files = getFiles("temp");
        files = filterFiles(files, false, "temp/org/h2/dev/*");
        files = filterFiles(files, false, "temp/org/h2/build/*");
        files = filterFiles(files, false, "temp/org/h2/samples/*");
        files = filterFiles(files, false, "temp/org/h2/test/*");
        files = filterFiles(files, false, "*.bat");
        files = filterFiles(files, false, "*.sh");
        files = filterFiles(files, false, "*.txt");
        jar("bin/h2.jar", "temp", files);
    }
    
    public void jarSmall() {
        compile(false);
        List files = getFiles("temp");
        files = filterFiles(files, false, "temp/org/h2/dev/*");
        files = filterFiles(files, false, "temp/org/h2/build/*");
        files = filterFiles(files, false, "temp/org/h2/samples/*");
        files = filterFiles(files, false, "temp/org/h2/test/*");
        files = filterFiles(files, false, "*.bat");
        files = filterFiles(files, false, "*.sh");
        files = filterFiles(files, false, "*.txt");
        files = filterFiles(files, false, "temp/META-INF/*");
        zip("temp/h2classes.zip", "temp", files, true, true);
        manifest("org.h2.tools.Console\nClass-Path: h2classes.zip");
        files = getFiles("temp/h2classes.zip");
        files.addAll(getFiles("temp/META-INF"));
        jar("bin/h2small.jar", "temp", files);
    }

    public void download() {
        download("ext/servlet-api-2.4.jar",
                "http://repo1.maven.org/maven2/javax/servlet/servlet-api/2.4/servlet-api-2.4.jar",
                "3fc542fe8bb8164e8d3e840fe7403bc0518053c0");
        download("ext/lucene-core-2.2.0.jar",
                "http://repo1.maven.org/maven2/org/apache/lucene/lucene-core/2.2.0/lucene-core-2.2.0.jar",
                "47b6eee2e17bd68911e7045896a1c09de0b2dda8");
    }

    public void compile() {
        compile(true);
    }
    
    private void compile(boolean debugInfo) {
        try {
            SwitchSource.main(new String[] { "-dir", "src", "-auto" });
        } catch (IOException e) {
            throw new Error(e);
        }
        clean();
        mkdir("temp");
        resources();
        download();

        String classpath = "temp" + File.pathSeparatorChar + "ext/servlet-api-2.4.jar" + File.pathSeparatorChar
                + "ext/lucene-core-2.2.0.jar" + File.pathSeparator + System.getProperty("java.home")
                + "/../lib/tools.jar";

        List files = getFiles("src/main");
        if (debugInfo) {
            javac(new String[] { "-d", "temp", "-sourcepath", "src/main", "-classpath", classpath }, files);
        } else {
            javac(new String[] { "-g:none", "-d", "temp", "-sourcepath", "src/main", "-classpath", classpath }, files);
        }

        files = getFiles("src/test");
        files.addAll(getFiles("src/tools"));
        javac(new String[] { "-d", "temp", "-sourcepath", "src/test" + File.pathSeparator + "src/tools",
                "-classpath", classpath }, files);

        files = getFiles("src/main/META-INF/services");
        copy("temp", files, "src/main");

        files = getFiles("src/installer");
        files = filterFiles(files, true, "*.bat");
        files = filterFiles(files, true, "*.sh");
        copy("temp", files, "src/installer");

        files = getFiles("src/test");
        files = filterFiles(files, false, "*.java");
        files = filterFiles(files, false, "*/package.html");
        copy("temp", files, "src/test");
    }

    public void clean() {
        delete("temp");
        delete("docs");
        mkdir("docs");
        mkdir("bin");
    }

    public void test() {

    }
}
