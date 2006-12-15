/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools.code;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.RandomAccessFile;

import org.h2.util.ByteUtils;

public class CheckTextFiles {
    public static void main(String[] args) throws Exception {
        new CheckTextFiles().run();
    }

    String[] suffixCheck = new String[]{"html", "jsp", "js", "css", "bat", "nsi", "java", "txt", "properties", "cpp", "def", "h", "rc", "dev", "sql", "xml", "csv", "Driver"};
    String[] suffixIgnore = new String[]{"gif", "png", "odg", "ico", "sxd", "layout", "res", "win", "dll", "jar"};
    boolean failOnError;
    boolean allowTab, allowCR = true, allowTrailingSpaces = true;
    int spacesPerTab = 4;
    boolean autoFix = true;
    boolean useCRLF = true;
    // must contain "+" otherwise this here counts as well
    String copyrightLicense = "Copyright 2004-2006 H2 Group. "+"Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).";
    String[] suffixIgnoreLicense = new String[]{"bat", "nsi", "txt", "properties", "def", "rc", "dev", "xml", "_private.h", "java.sql.Driver"};
    boolean hasError;

    void run() throws Exception {
        String baseDir = "src";
        check(new File(baseDir));
        if(hasError) {
            throw new Exception("Errors found");
        }
    }

    private void check(File file) throws Exception {
        String name = file.getName();
        if(file.isDirectory()) {
            if(name.equals("CVS") || name.equals(".svn")) {
                return;
            }
            File[] list = file.listFiles();
            for(int i=0; i<list.length; i++) {
                check(list[i]);
            }
        } else {
            String suffix = "";
            int lastDot = name.lastIndexOf('.');
            if(lastDot >= 0) {
                suffix = name.substring(lastDot+1);
            }
            boolean check = false, ignore = false;
            for(int i=0; i<suffixCheck.length; i++) {
                if(suffix.equals(suffixCheck[i])) {
                    check = true;
                }
            }
            for(int i=0; i<suffixIgnore.length; i++) {
                if(suffix.equals(suffixIgnore[i])) {
                    ignore = true;
                }
            }
            boolean checkLicense = true;
            for(int i=0; i<suffixIgnoreLicense.length; i++) {
                String ig = suffixIgnoreLicense[i];
                if(suffix.equals(ig) || name.endsWith(ig)) {
                    checkLicense = false;
                    break;
                }
            }
            if(ignore == check) {
                throw new Error("Unknown suffix: " + suffix + " for file: " + name);
            }
            if(check) {
                checkOrFixFile(file, autoFix, checkLicense);
            }
        }
    }

    void checkOrFixFile(File file, boolean fix, boolean checkLicense) throws Exception {
        RandomAccessFile in = new RandomAccessFile(file, "r");
        byte[] data = new byte[(int)file.length()];
        ByteArrayOutputStream out = fix ? new ByteArrayOutputStream() : null;
        in.readFully(data);
        in.close();
        if(checkLicense) {
            if(data.length > copyrightLicense.length()) {
                // don't check tiny files
                String text = new String(data);
                if(text.indexOf(copyrightLicense) < 0) {
                    fail(file, "license is missing", 0);
                }
            }
        }
        int line = 1;
        boolean lastWasWhitespace = false;
        for(int i=0; i<data.length; i++) {
            char ch = (char) (data[i] & 0xff);
            if(ch > 127) {
                fail(file, "contains character "+ch, line);
                return;
            } else if(ch < 32) {
                if(ch == '\n') {
                    if(lastWasWhitespace && !allowTrailingSpaces) {
                        fail(file, "contains trailing white space", line);
                        return;
                    }
                    if(fix) {
                        if(useCRLF) {
                            out.write('\r');
                        }
                        out.write(ch);
                    }
                    lastWasWhitespace = false;
                    line++;
                } else if(ch == '\r') {
                    if(!allowCR) {
                        fail(file, "contains CR", line);
                        return;
                    }
                    if(lastWasWhitespace && !allowTrailingSpaces) {
                        fail(file, "contains trailing white space", line);
                        return;
                    }
                    lastWasWhitespace = false;
                    // ok
                } else if(ch == '\t') {
                    if(fix) {
                        for(int j=0; j<spacesPerTab; j++) {
                            out.write(' ');
                        }
                    } else {
                        if(!allowTab) {
                            fail(file, "contains TAB", line);
                            return;
                        }
                    }
                    lastWasWhitespace = true;
                    // ok
                } else {
                    fail(file, "contains character "+(int)ch, line);
                    return;
                }
            } else {
                if(fix) {
                    out.write(ch);
                }
                lastWasWhitespace = Character.isWhitespace(ch);
            }
        }
        if(lastWasWhitespace && !allowTrailingSpaces) {
            fail(file, "contains trailing white space at the very end", line);
            return;
        }
        if(fix) {
            byte[] changed = out.toByteArray();
            if(ByteUtils.compareNotNull(data, changed) != 0) {
                RandomAccessFile f = new RandomAccessFile(file, "rw");
                f.write(changed);
                f.setLength(changed.length);
                f.close();
                System.out.println("CHANGED: File " + file.getName());
            }
        }
    }

    private void fail(File file, String error, int line) {
        System.out.println("FAIL: File " + file.getAbsolutePath() + " " + error + " at line " + line);
        hasError = true;
        if(failOnError) {
            throw new Error("FAIL");
        }
    }

}
