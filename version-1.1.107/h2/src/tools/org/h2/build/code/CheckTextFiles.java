/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.code;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.RandomAccessFile;

import org.h2.util.ByteUtils;

/**
 * This tool checks that source code files only contain the allowed set of
 * characters, and that the copyright license is included in each file. It also
 * removes trailing spaces.
 */
public class CheckTextFiles {

    // must contain "+" otherwise this here counts as well
    private static final String COPYRIGHT = "Copyright 2004-2009 " + "H2 Group.";
    private static final String LICENSE = "Multiple-Licensed " + "under the H2 License";

    private static final String[] SUFFIX_CHECK = new String[] { "html", "jsp", "js", "css", "bat", "nsi",
            "java", "txt", "properties", "sql", "xml", "csv", "Driver" };
    private static final String[] SUFFIX_IGNORE = new String[] { "gif", "png", "odg", "ico", "sxd",
            "layout", "res", "win", "jar", "task", "svg", "MF", "sh", "DS_Store" };

    private boolean failOnError;
    private boolean allowTab, allowCR = true, allowTrailingSpaces;
    private int spacesPerTab = 4;
    private boolean autoFix = true;
    private boolean useCRLF;
    private String[] suffixIgnoreLicense = new String[] { "bat", "nsi", "txt", "properties", "xml", "java.sql.Driver", "task", "sh" };
    private boolean hasError;

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String[] args) throws Exception {
        new CheckTextFiles().run();
    }

    private void run() throws Exception {
        String baseDir = "src";
        check(new File(baseDir));
        if (hasError) {
            throw new Exception("Errors found");
        }
    }

    private void check(File file) throws Exception {
        String name = file.getName();
        if (file.isDirectory()) {
            if (name.equals("CVS") || name.equals(".svn")) {
                return;
            }
            File[] list = file.listFiles();
            for (int i = 0; i < list.length; i++) {
                check(list[i]);
            }
        } else {
            String suffix = "";
            int lastDot = name.lastIndexOf('.');
            if (lastDot >= 0) {
                suffix = name.substring(lastDot + 1);
            }
            boolean check = false, ignore = false;
            for (int i = 0; i < SUFFIX_CHECK.length; i++) {
                if (suffix.equals(SUFFIX_CHECK[i])) {
                    check = true;
                }
            }
//            if (name.endsWith(".html") && name.indexOf("_ja") > 0) {
//                int todoRemoveJapaneseFiles;
//                // Japanese html files are UTF-8 at this time
//                check = false;
//                ignore = true;
//            }
            if (name.endsWith(".utf8.txt")) {
                check = false;
                ignore = true;
            }
            for (int i = 0; i < SUFFIX_IGNORE.length; i++) {
                if (suffix.equals(SUFFIX_IGNORE[i])) {
                    ignore = true;
                }
            }
            boolean checkLicense = true;
            for (int i = 0; i < suffixIgnoreLicense.length; i++) {
                String ig = suffixIgnoreLicense[i];
                if (suffix.equals(ig) || name.endsWith(ig)) {
                    checkLicense = false;
                    break;
                }
            }
            if (ignore == check) {
                throw new Error("Unknown suffix: " + suffix + " for file: " + file.getAbsolutePath());
            }
            if (check) {
                checkOrFixFile(file, autoFix, checkLicense);
            }
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
    public void checkOrFixFile(File file, boolean fix, boolean checkLicense) throws Exception {
        RandomAccessFile in = new RandomAccessFile(file, "r");
        byte[] data = new byte[(int) file.length()];
        ByteArrayOutputStream out = fix ? new ByteArrayOutputStream() : null;
        in.readFully(data);
        in.close();
        if (checkLicense) {
            if (data.length > COPYRIGHT.length() + LICENSE.length()) {
                // don't check tiny files
                String text = new String(data);
                if (text.indexOf(COPYRIGHT) < 0) {
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
        boolean lastWasWhitespace = false;
        for (int i = 0; i < data.length; i++) {
            char ch = (char) (data[i] & 0xff);
            if (ch > 127) {
                fail(file, "contains character " + ch, line);
                return;
            } else if (ch < 32) {
                if (ch == '\n') {
                    if (lastWasWhitespace && !allowTrailingSpaces) {
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
                } else if (ch == '\r') {
                    if (!allowCR) {
                        fail(file, "contains CR", line);
                        return;
                    }
                    if (lastWasWhitespace && !allowTrailingSpaces) {
                        fail(file, "contains trailing white space", line);
                        return;
                    }
                    lastWasWhitespace = false;
                    // ok
                } else if (ch == '\t') {
                    if (fix) {
                        for (int j = 0; j < spacesPerTab; j++) {
                            out.write(' ');
                        }
                    } else {
                        if (!allowTab) {
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
            } else {
                if (fix) {
                    out.write(ch);
                }
                lastWasWhitespace = Character.isWhitespace(ch);
            }
        }
        if (lastWasWhitespace && !allowTrailingSpaces) {
            fail(file, "contains trailing white space at the very end", line);
            return;
        }
        if (fix) {
            byte[] changed = out.toByteArray();
            if (ByteUtils.compareNotNull(data, changed) != 0) {
                RandomAccessFile f = new RandomAccessFile(file, "rw");
                f.write(changed);
                f.setLength(changed.length);
                f.close();
                System.out.println("CHANGED: File " + file.getName());
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
                            fail(file, "contains wrong number of heading spaces: " + (j - i - 1), line);
                        }
                        break;
                    }
                }
            }
        }
    }

    private void fail(File file, String error, int line) {
        if (line <= 0) {
            line = 1;
        }
        String name = file.getAbsolutePath();
        int idx = name.lastIndexOf(File.separatorChar);
        if (idx >= 0) {
            name = name.replace(File.separatorChar, '.');
            name = name + "(" + name.substring(idx + 1) + ":" + line + ")";
            idx = name.indexOf("org.");
            if (idx > 0) {
                name = name.substring(idx);
            }
        }
        System.out.println("FAIL at " + name + " " + error + " " + file.getAbsolutePath());
        hasError = true;
        if (failOnError) {
            throw new Error("FAIL");
        }
    }

}
