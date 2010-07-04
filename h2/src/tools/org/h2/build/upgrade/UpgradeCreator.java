/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.upgrade;

import java.io.File;
import org.h2.build.BuildBase;

/**
 * Creates the v 1.1 upgrade sources
 */
public class UpgradeCreator {    
    
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: java -cp . org.h2.build.upgrade.UpgradeCreator <srcdir> <destdir>");
            System.exit(1);            
        }
        File srcDir = new File(args[0]);
        if (!srcDir.exists()) {
            System.out.println("Source dir does not exist");
            System.exit(1);            
        }
        File destDir = new File(args[1]);
        if (destDir.exists()) {
            System.out.println("Destination dir already exists");
            System.exit(1);            
        }
        destDir.mkdirs();
        convert(srcDir, srcDir, destDir);
    }
    
    private static void convert(File file, File srcDir, File destDir) throws Exception {
        String pathInDestDir = file.getCanonicalPath().substring(srcDir.getCanonicalPath().length());
        
        pathInDestDir = pathInDestDir.replaceAll("org" + File.separator + "h2", 
                "org" + File.separator + 
                "h2" + File.separator + 
                "upgrade" + File.separator + 
                "v1_1");
        File fileInDestDir = new File(destDir, pathInDestDir);
//        System.out.println(fileInDestDir.getAbsoluteFile());
        if (file.isDirectory()) {
            fileInDestDir.mkdirs();
            File[] files = file.listFiles();
            for (File child : files) {
                convert(child, srcDir, destDir);
            }
        } else {
            byte[] content = BuildBase.readFile(file);
            String contentString = new String(content);
            if (file.getName().endsWith(".java")) {
                contentString = replaceInJavaFile(file, contentString);
            }
            if (file.getName().equals("build.sh") || file.getName().equals("build.bat") || file.getName().equals("build.xml")) {
                contentString = contentString.replaceAll("org\\.h2\\.build", "org.h2.upgrade.v1_1.build");
                contentString = contentString.replaceAll("org/h2/", "org/h2/upgrade/v1_1/");
            }
            content = contentString.getBytes();
            BuildBase.writeFile(fileInDestDir, content);            
        }
    }
    
    private static String replaceInJavaFile(File file, String content) {
        content = content.replaceAll("import org\\.h2", "import org.h2.upgrade.v1_1");
        content = content.replaceAll("import static org\\.h2", "import static org.h2.upgrade.v1_1");
        content = content.replaceAll("package org\\.h2", "package org.h2.upgrade.v1_1");
        content = content.replaceAll("org\\.h2\\.samples", "org.h2.upgrade.v1_1.samples");
        content = content.replaceAll("org\\.h2\\.tools", "org.h2.upgrade.v1_1.tools");
        content = content.replaceAll("org\\.h2\\.mode", "org.h2.upgrade.v1_1.mode");
        content = content.replaceAll("org\\.h2\\.Driver\\.load\\(\\)", "org.h2.upgrade.v1_1.Driver.load()");
        content = content.replaceAll("org\\.h2\\.Driver\\.unload\\(\\)", "org.h2.upgrade.v1_1.Driver.unload()");

        if (file.getName().equals("ConnectionInfo.java")) {
            content = content.replaceAll("boolean isPersistent\\(\\) \\{", "public boolean isPersistent() {");
            content = content.replaceAll("String getName\\(\\) throws SQLException \\{", "public String getName() throws SQLException {");
        }
        if (file.getName().equals("Constants.java")) {
            content = content.replaceAll("public static final String START_URL = \"jdbc:h2:\";", "public static final String START_URL = \"jdbc:h2v1_1:\";");
        }
        if (file.getName().equals("SessionRemote.java")) {
            content = content.replaceAll("org\\.h2\\.engine\\.SessionFactoryEmbedded", "org.h2.upgrade.v1_1.engine.SessionFactoryEmbedded");
        }
        
        return content;
    }    
    
}

