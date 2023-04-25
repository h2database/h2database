/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.code;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Enable / disable AB-BA deadlock detector code.
 */
public class AbbaDetect {

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        String baseDir = "src/main";
        Files.walkFileTree(Paths.get(baseDir), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                process(file, true);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * Process a file.
     *
     * @param file the file
     */
    static void process(Path file, boolean enable) throws IOException {
        String name = file.getFileName().toString();
        if (!name.endsWith(".java")) {
            return;
        }
        if (name.endsWith("AbbaDetector.java")) {
            return;
        }
        String source = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        String original = source;

        source = disable(source);
        if (enable) {
            String s2 = enable(source);
            if (!source.equals(disable(s2))) {
                throw new IOException("Could not revert changes for file " + file);
            }
            source = s2;
        }

        if (source.equals(original)) {
            return;
        }
        Path newFile = Paths.get(file.toString() + ".new");
        Files.write(newFile, source.getBytes(StandardCharsets.UTF_8));

        Path oldFile = Paths.get(file.toString() + ".old");
        Files.move(file, oldFile);
        Files.move(newFile, file);
        Files.delete(oldFile);
    }

    private static String disable(String source) {
        source = source.replaceAll("\\{org.h2.util.AbbaDetector.begin\\(.*\\);", "{");
        source = source.replaceAll("org.h2.util.AbbaDetector.begin\\((.*\\(\\))\\)", "$1");
        source = source.replaceAll("org.h2.util.AbbaDetector.begin\\((.*)\\)", "$1");
        source = source.replaceAll("synchronized  ", "synchronized ");
        return source;
    }

    private static String enable(String source) {
        // the word synchronized within single line comments
        source = source.replaceAll("(// .* synchronized )([^ ])", "$1 $2");

        source = source.replaceAll("synchronized \\((.*)\\(\\)\\)",
                "synchronized  \\(org.h2.util.AbbaDetector.begin\\($1\\(\\)\\)\\)");
        source = source.replaceAll("synchronized \\((.*)\\)",
                "synchronized  \\(org.h2.util.AbbaDetector.begin\\($1\\)\\)");

        source = source.replaceAll("static synchronized ([^ (].*)\\{",
                "static synchronized  $1{org.h2.util.AbbaDetector.begin\\(null\\);");
        source = source.replaceAll("synchronized ([^ (].*)\\{",
                "synchronized  $1{org.h2.util.AbbaDetector.begin\\(this\\);");

        return source;
    }

}
