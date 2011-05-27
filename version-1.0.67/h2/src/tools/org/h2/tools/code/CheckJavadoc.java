/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools.code;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * This tool checks that for each .java file there is a package.html file,
 * and that for each .java file there is at least one (class level) javadoc comment.
 */
public class CheckJavadoc {

    private boolean hasError;

    public static void main(String[] args) throws Exception {
        new CheckJavadoc().run();
    }

    void run() throws Exception {
        String baseDir = "src";
        check(new File(baseDir));
        if (hasError) {
            throw new Exception("Errors found");
        }
    }

    private int check(File file) throws Exception {
        String name = file.getName();
        if (file.isDirectory()) {
            if (name.equals("CVS") || name.equals(".svn")) {
                return 0;
            }
            File[] list = file.listFiles();
            boolean foundPackageHtml = false, foundJava = false;
            for (int i = 0; i < list.length; i++) {
                int type = check(list[i]);
                if (type == 1) {
                    foundJava = true;
                } else if (type == 2) {
                    foundPackageHtml = true;
                }
            }
            if (foundJava && !foundPackageHtml) {
                System.out.println("No package.html file, but a Java file found at: " + file.getAbsolutePath());
                hasError = true;
            }
        } else {
            if (name.endsWith(".java")) {
                checkJavadoc(file);
                return 1;
            } else if (name.equals("package.html")) {
                return 2;
            }
        }
        return 0;
    }

    private void checkJavadoc(File file) throws IOException {
        RandomAccessFile in = new RandomAccessFile(file, "r");
        byte[] data = new byte[(int) file.length()];
        in.readFully(data);
        in.close();
        String text = new String(data);
        int comment = text.indexOf("/**");
        if (comment < 0) {
            System.out.println("No Javadoc comment: " + file.getAbsolutePath());
            hasError = true;
        }
        int open = text.indexOf('{');
        if (open < 0 || open < comment) {
            System.out.println("No '{' or '{' before the first Javadoc comment: " + file.getAbsolutePath());
            hasError = true;
        }
    }

}
