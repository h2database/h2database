package org.h2.build.doc;
/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.h2.dev.ftp.FtpClient;
import org.h2.util.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;

/**
 * Upload the code coverage result to the H2 web site.
 */
public class UploadCoverage {

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String[] args) throws Exception {
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
        FileUtils.delete("coverage.txt");
        FileUtils.delete("details/coverage_files.zip");
        FileUtils.delete("details");
        String password = System.getProperty("h2.ftpPassword");
        if (password != null) {
            FtpClient ftp = FtpClient.open("h2database.com");
            ftp.login("h2database", password);
            ftp.changeWorkingDirectory("/httpdocs");
            if (ftp.exists("/httpdocs", "coverage")) {
                ftp.removeDirectoryRecursive("/httpdocs/coverage");
            }
            ftp.makeDirectory("/httpdocs/coverage");
            ftp.store("/httpdocs/coverage/overview.html", new FileInputStream("coverage/overview.html"));
            ftp.store("/httpdocs/coverage/coverage.zip", new FileInputStream("coverage.zip"));
            ftp.close();
            FileUtils.delete("coverage.zip");
        }
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
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                addFiles(base, files[i], out);
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
