/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.doc;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.h2.dev.ftp.FtpClient;
import org.h2.engine.Constants;
import org.h2.test.utils.OutputCatcher;
import org.h2.tools.RunScript;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;

/**
 * Upload the code coverage result to the H2 web site.
 */
public class UploadBuild {

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        System.setProperty("h2.socketConnectTimeout", "30000");
        byte[] data = IOUtils.readBytesAndClose(new FileInputStream("coverage/index.html"), -1);
        String index = new String(data, "ISO-8859-1");
        while (true) {
            int idx = index.indexOf("<A HREF=\"");
            if (idx < 0) {
                break;
            }
            int end = index.indexOf('>', idx) + 1;
            index = index.substring(0, idx) + index.substring(end);
            idx = index.indexOf("</A>");
            index = index.substring(0, idx) + index.substring(idx + "</A>".length());
        }
        index = StringUtils.replaceAll(index, "[all", "");
        index = StringUtils.replaceAll(index, "classes]", "");
        FileOutputStream out = new FileOutputStream("coverage/overview.html");
        out.write(index.getBytes("ISO-8859-1"));
        out.close();
        new File("details").mkdir();
        zip("details/coverage_files.zip", "coverage", true);
        zip("coverage.zip", "details", false);
        IOUtils.delete("coverage.txt");
        IOUtils.delete("details/coverage_files.zip");
        IOUtils.delete("details");
        String password = System.getProperty("h2.ftpPassword");
        if (password == null) {
            return;
        }
        FtpClient ftp = FtpClient.open("h2database.com");
        ftp.login("h2database", password);
        ftp.changeWorkingDirectory("/httpdocs");
        if (ftp.exists("/httpdocs", "coverage")) {
            ftp.removeDirectoryRecursive("/httpdocs/coverage");
        }
        ftp.makeDirectory("/httpdocs/coverage");
        String testOutput = IOUtils.readStringAndClose(new FileReader("docs/html/testOutput.html"), -1);
        boolean error = testOutput.indexOf(OutputCatcher.START_ERROR) >= 0;
        if (!ftp.exists("/httpdocs", "automated")) {
            ftp.makeDirectory("/httpdocs/automated");
        }
        String buildSql;
        if (ftp.exists("/httpdocs/automated", "history.sql")) {
            buildSql = new String(ftp.retrieve("/httpdocs/automated/history.sql"));
        } else {
            buildSql = "create table item(id identity, title varchar, issued timestamp, desc varchar);\n";
        }
        String ts = new java.sql.Timestamp(System.currentTimeMillis()).toString();
        String now = ts.substring(0, 16);
        int idx = testOutput.indexOf("Statements per second: ");
        if (idx >= 0) {
            int end = testOutput.indexOf("<br />", idx);
            if (end >= 0) {
                String result = testOutput.substring(idx + "Statements per second: ".length(), end);
                now += " (" + result + " op/s)";
            }
        }
        String sql = "insert into item(title, issued, desc) values('Build " + now + (error ? " FAILED" : "") +
            "', '" + ts + "', '<a href=\"http://www.h2database.com/html/testOutput.html\">Output</a>" +
            " - <a href=\"http://www.h2database.com/coverage/overview.html\">Coverage</a>" +
            " - <a href=\"http://www.h2database.com/automated/h2-latest.jar\">Jar</a>');\n";
        buildSql += sql;
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:");
        RunScript.execute(conn, new StringReader(buildSql));
        InputStream in = new FileInputStream("src/tools/org/h2/build/doc/buildNewsfeed.sql");
        ResultSet rs = RunScript.execute(conn, new InputStreamReader(in, "ISO-8859-1"));
        in.close();
        rs.next();
        String content = rs.getString("content");
        conn.close();
        ftp.store("/httpdocs/automated/history.sql", new ByteArrayInputStream(buildSql.getBytes()));
        ftp.store("/httpdocs/automated/news.xml", new ByteArrayInputStream(content.getBytes()));
        ftp.store("/httpdocs/html/testOutput.html", new ByteArrayInputStream(testOutput.getBytes()));
        ftp.store("/httpdocs/coverage/overview.html", new FileInputStream("coverage/overview.html"));
        String jarFileName = "bin/h2-" + Constants.getVersion() + ".jar";
        if (IOUtils.exists(jarFileName)) {
            ftp.store("/httpdocs/automated/h2-latest.jar", new FileInputStream(jarFileName));
        }
        ftp.store("/httpdocs/coverage/coverage.zip", new FileInputStream("coverage.zip"));
        ftp.close();
        IOUtils.delete("coverage.zip");
    }

    private static void zip(String destFile, String directory, boolean storeOnly) throws IOException {
        OutputStream out = new FileOutputStream(destFile);
        ZipOutputStream zipOut = new ZipOutputStream(out);
        if (storeOnly) {
            zipOut.setMethod(ZipOutputStream.STORED);
        }
        zipOut.setLevel(Deflater.BEST_COMPRESSION);
        addFiles(new File(directory), new File(directory), zipOut);
        zipOut.finish();
        zipOut.close();
    }

    private static void addFiles(File base, File file, ZipOutputStream out) throws IOException {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                addFiles(base, f, out);
            }
        } else {
            String path = file.getAbsolutePath().substring(base.getAbsolutePath().length());
            path = path.replace('\\', '/');
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
            byte[] data = IOUtils.readBytesAndClose(new FileInputStream(file), -1);
            ZipEntry entry = new ZipEntry(path);
            CRC32 crc = new CRC32();
            crc.update(data);
            entry.setSize(file.length());
            entry.setCrc(crc.getValue());
            out.putNextEntry(entry);
            out.write(data);
            out.closeEntry();
        }
    }

}
