/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.code;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * This tool checks that for each .java file there is a package.html file,
 * that for each .java file there is at least one (class level) javadoc comment,
 * and that lines with comments are not too long.
 */
public class CheckJavadoc {

    private static final int MAX_COMMENT_LINE_SIZE = 100;
    private static final int MAX_SOURCE_LINE_SIZE = 120;
    private int errorCount;

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        new CheckJavadoc().run();
    }

    private void run() throws Exception {
        check(Paths.get("src"));
        if (errorCount > 0) {
            throw new Exception(errorCount + " errors found");
        }
    }

    private int check(Path file) throws Exception {
        String name = file.getFileName().toString();
        if (Files.isDirectory(file)) {
            boolean foundPackageHtml = false, foundJava = false;
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(file)) {
                for (Path f : stream) {
                    int type = check(f);
                    if (type == 1) {
                        foundJava = true;
                    } else if (type == 2) {
                        foundPackageHtml = true;
                    }
                }
            }
            if (foundJava && !foundPackageHtml) {
                System.out.println("No package-info.java file, but a Java file found at: " + file.toAbsolutePath());
                errorCount++;
            }
        } else {
            if (name.endsWith(".java")) {
                checkJavadoc(file);
                return name.equals("package-info.java") ? 2 : 1;
            }
        }
        return 0;
    }

    private void checkJavadoc(Path file) throws IOException {
        List<String> lines = Files.readAllLines(file);
        boolean inComment = false, hasJavadoc = false;
        for (int lineNumber = 0, size = lines.size(); lineNumber < size;) {
            String rawLine = lines.get(lineNumber++);
            String line = rawLine.trim();
            if (line.startsWith("/*")) {
                if (!hasJavadoc && line.startsWith("/**")) {
                    hasJavadoc = true;
                }
                inComment = true;
            }
            int rawLength = rawLine.length();
            if (inComment) {
                int i = line.indexOf("*/", 2);
                if (i >= 0) {
                    inComment = false;
                }
                if (i == rawLength - 2 && rawLength > MAX_COMMENT_LINE_SIZE
                        && !line.trim().startsWith("* http://")
                        && !line.trim().startsWith("* https://")) {
                    System.out.println("Long line: " + file.toAbsolutePath()
                            + " (" + file.getFileName() + ":" + lineNumber + ")");
                    errorCount++;
                }
            }
            if (!inComment && line.startsWith("//")) {
                if (rawLength > MAX_COMMENT_LINE_SIZE
                        && !line.trim().startsWith("// http://")
                        && !line.trim().startsWith("// https://")) {
                    System.out.println("Long line: " + file.toAbsolutePath()
                            + " (" + file.getFileName() + ":" + lineNumber + ")");
                    errorCount++;
                }
            } else if (!inComment && rawLength > MAX_SOURCE_LINE_SIZE) {
                System.out.println("Long line: " + file.toAbsolutePath()
                        + " (" + file.getFileName() + ":" + lineNumber + ")");
                errorCount++;
            }
        }
        if (!hasJavadoc) {
            System.out.println("No Javadoc comment: " + file.toAbsolutePath());
            errorCount++;
        }
    }

}
