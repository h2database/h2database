/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.code;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;

/**
 * This tool checks that source code files only contain the allowed set of
 * characters, and that the copyright license is included in each file. It also
 * removes trailing spaces.
 */
public class CheckTextFiles {

    private static final int MAX_SOURCE_LINE_SIZE = 120;

    // must contain "+" otherwise this here counts as well
    private static final String COPYRIGHT1 = "Copyright 2004-2023";
    private static final String COPYRIGHT2 = "H2 Group.";
    private static final String LICENSE = "Multiple-Licensed " +
            "under the MPL 2.0";

    private static final String[] SUFFIX_CHECK = { "html", "jsp", "js", "css",
            "bat", "nsi", "java", "txt", "properties", "sql", "xml", "csv",
            "Driver", "Processor", "prefs" };
    private static final String[] SUFFIX_IGNORE = { "gif", "png", "odg", "ico",
            "sxd", "layout", "res", "win", "jar", "task", "svg", "MF", "mf",
            "sh", "DS_Store", "prop", "class", "json" };
    private static final String[] SUFFIX_CRLF = { "bat" };

    private static final boolean ALLOW_TAB = false;
    private static final boolean ALLOW_CR = true;
    private static final boolean ALLOW_TRAILING_SPACES = false;
    private static final int SPACES_PER_TAB = 4;
    private static final boolean AUTO_FIX = true;

    private boolean failOnError;
    private boolean useCRLF;
    private final String[] suffixIgnoreLicense = {
            "bat", "nsi", "txt", "properties", "xml",
            "java.sql.Driver", "task", "sh", "prefs" };
    private boolean hasError;

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        new CheckTextFiles().run();
    }

    private void run() throws Exception {
        Files.walkFileTree(Paths.get("src"), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                check(file);
                return FileVisitResult.CONTINUE;
            }
        });
        if (hasError) {
            throw new Exception("Errors found");
        }
    }

    void check(Path file) throws IOException {
        String name = file.getFileName().toString();
        String suffix = "";
        int lastDot = name.lastIndexOf('.');
        if (lastDot >= 0) {
            suffix = name.substring(lastDot + 1);
        }
        boolean check = false, ignore = false;
        for (String s : SUFFIX_CHECK) {
            if (suffix.equals(s)) {
                check = true;
            }
        }
        for (String s : SUFFIX_IGNORE) {
            if (suffix.equals(s)) {
                ignore = true;
            }
        }
        boolean checkLicense = true;
        for (String ig : suffixIgnoreLicense) {
            if (suffix.equals(ig) || name.endsWith(ig)) {
                checkLicense = false;
                break;
            }
        }
        if (ignore == check) {
            throw new RuntimeException("Unknown suffix: " + suffix
                    + " for file: " + file.toAbsolutePath());
        }
        useCRLF = false;
        for (String s : SUFFIX_CRLF) {
            if (suffix.equals(s)) {
                useCRLF = true;
                break;
            }
        }
        if (check) {
            checkOrFixFile(file, AUTO_FIX, checkLicense);
        }
    }

    /**
     * Check a source code file. The following properties are checked:
     * copyright, license, incorrect source switches, trailing white space,
     * newline characters, tab characters, and characters codes (only characters
     * below 128 are allowed).
     *
     * @param file the file to check
     * @param fix automatically fix newline characters and trailing spaces
     * @param checkLicense check the license and copyright
     */
    public void checkOrFixFile(Path file, boolean fix, boolean checkLicense) throws IOException {
        byte[] data = Files.readAllBytes(file);
        ByteArrayOutputStream out = fix ? new ByteArrayOutputStream() : null;
        if (checkLicense) {
            if (data.length > COPYRIGHT1.length() + LICENSE.length()) {
                // don't check tiny files
                String text = new String(data);
                if (text.indexOf(COPYRIGHT1) < 0) {
                    fail(file, "copyright is missing", 0);
                }
                if (text.indexOf(COPYRIGHT2) < 0) {
                    fail(file, "copyright is missing", 0);
                }
                if (text.indexOf(LICENSE) < 0) {
                    fail(file, "license is missing", 0);
                }
                if (text.indexOf("// " + "##") > 0) {
                    fail(file, "unexpected space between // and ##", 0);
                }
                if (text.indexOf("/* " + "##") > 0) {
                    fail(file, "unexpected space between /* and ##", 0);
                }
                if (text.indexOf("##" + " */") > 0) {
                    fail(file, "unexpected space between ## and */", 0);
                }
            }
        }
        int line = 1;
        int startLinePos = 0;
        boolean lastWasWhitespace = false;
        for (int i = 0; i < data.length; i++) {
            char ch = (char) (data[i] & 0xff);
            boolean isWhitespace = Character.isWhitespace(ch);
            if (ch > 127) {
                fail(file, "contains character " + (int) ch + " at "
                        + new String(data, i - 10, 20), line);
                return;
            } else if (ch < 32) {
                if (ch == '\n') {
                    if (lastWasWhitespace && !ALLOW_TRAILING_SPACES) {
                        fail(file, "contains trailing white space", line);
                        return;
                    }
                    if (fix) {
                        if (useCRLF) {
                            out.write('\r');
                        }
                        out.write(ch);
                    }
                    lastWasWhitespace = false;
                    line++;
                    int lineLength = i - startLinePos;
                    if (file.getFileName().toString().endsWith(".java")) {
                        if (i > 0 && data[i - 1] == '\r') {
                            lineLength--;
                        }
                        if (lineLength > MAX_SOURCE_LINE_SIZE) {
                            String s = new String(data, startLinePos, lineLength).trim();
                            if (!s.startsWith("// http://") && !s.startsWith("// https://")) {
                                fail(file, "line too long: " + lineLength, line);
                            }
                        }
                    }
                    startLinePos = i;
                } else if (ch == '\r') {
                    if (!ALLOW_CR) {
                        fail(file, "contains CR", line);
                        return;
                    }
                    if (lastWasWhitespace && !ALLOW_TRAILING_SPACES) {
                        fail(file, "contains trailing white space", line);
                        return;
                    }
                    lastWasWhitespace = false;
                    // ok
                } else if (ch == '\t') {
                    if (fix) {
                        for (int j = 0; j < SPACES_PER_TAB; j++) {
                            out.write(' ');
                        }
                    } else {
                        if (!ALLOW_TAB) {
                            fail(file, "contains TAB", line);
                            return;
                        }
                    }
                    lastWasWhitespace = true;
                    // ok
                } else {
                    fail(file, "contains character " + (int) ch, line);
                    return;
                }
            } else if (isWhitespace) {
                lastWasWhitespace = true;
                if (fix) {
                    boolean write = true;
                    for (int j = i + 1; j < data.length; j++) {
                        char ch2 = (char) (data[j] & 0xff);
                        if (ch2 == '\n' || ch2 == '\r') {
                            write = false;
                            lastWasWhitespace = false;
                            ch = ch2;
                            i = j - 1;
                            break;
                        } else if (!Character.isWhitespace(ch2)) {
                            break;
                        }
                    }
                    if (write) {
                        out.write(ch);
                    }
                }
            } else {
                if (fix) {
                    out.write(ch);
                }
                lastWasWhitespace = false;
            }
        }
        if (lastWasWhitespace && !ALLOW_TRAILING_SPACES) {
            fail(file, "contains trailing white space at the very end", line);
            return;
        }
        if (fix) {
            byte[] changed = out.toByteArray();
            if (!Arrays.equals(data, changed)) {
                Files.write(file, changed);
                System.out.println("CHANGED: " + file.getFileName());
            }
        }
        line = 1;
        for (int i = 0; i < data.length; i++) {
            if (data[i] < 32) {
                line++;
                for (int j = i + 1; j < data.length; j++) {
                    if (data[j] != 32) {
                        int mod = (j - i - 1) & 3;
                        if (mod != 0 && (mod != 1 || data[j] != '*')) {
                            fail(file, "contains wrong number " +
                                    "of heading spaces: " + (j - i - 1), line);
                        }
                        break;
                    }
                }
            }
        }
    }

    private void fail(Path file, String error, int line) {
        file = file.toAbsolutePath();
        if (line <= 0) {
            line = 1;
        }
        String name = file.toString();
        int idx = name.lastIndexOf(File.separatorChar);
        if (idx >= 0) {
            name = name.replace(File.separatorChar, '.');
            name = name + "(" + name.substring(idx + 1) + ":" + line + ")";
            idx = name.indexOf("org.");
            if (idx > 0) {
                name = name.substring(idx);
            }
        }
        System.out.println("FAIL at " + name + " " + error + " " + file.toAbsolutePath());
        hasError = true;
        if (failOnError) {
            throw new RuntimeException("FAIL");
        }
    }

}
