package org.h2.tools.code;

import java.io.File;

/**
 * This tool checks that for each .java file there is a package.html file.
 */
public class CheckPackageHtml {

    private boolean hasError;

    public static void main(String[] args) throws Exception {
        new CheckPackageHtml().run();
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
                return 1;
            } else if (name.equals("package.html")) {
                return 2;
            }
        }
        return 0;
    }

}
