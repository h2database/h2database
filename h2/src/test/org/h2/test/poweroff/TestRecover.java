package org.h2.test.poweroff;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Date;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.h2.util.IOUtils;

public class TestRecover {
	
    private Random random;
    private static final String RELATIVE = System.getProperty("h2.testRecoverPath", "db");
    private static final String TEST_DIRECTORY = "/temp/"+RELATIVE+"/data";
    private static final String BACKUP_DIRECTORY = "/temp/"+RELATIVE+"/last";

    public static void main(String[] args) throws Exception {
        new TestRecover().runTest(args);
    }
    
    private void runTest(String[] args) throws Exception {
        System.out.println("backup...");
        new File(TEST_DIRECTORY).mkdirs();
        File backup = backup(TEST_DIRECTORY, BACKUP_DIRECTORY, "data", 10);
        System.out.println("check consistency...");
        if(!testConsistency()) {
            System.out.println("error! renaming file");
            backup.renameTo(new File(backup.getParentFile(), "error-"+ backup.getName()));
        }
        System.out.println("deleting old run...");
        deleteRecursive(new File(TEST_DIRECTORY));
        System.out.println("testing...");
        testLoop();
    }
    
    static File backup(String sourcePath, String targetPath, String basePath, int max) throws Exception {
        File root = new File(targetPath);
        if(!root.exists()) {
            root.mkdirs();
        }
        while(true) {
            File[] list = root.listFiles();
            File oldest = null;
            int count = 0;
            for(int i=0; i<list.length; i++) {
                File f = list[i];
                String name = f.getName();
                if(f.isFile() && name.startsWith("backup") && name.endsWith(".zip")) {
                    count++;
                    if(oldest == null || f.lastModified() < oldest.lastModified()) {
                        oldest = f;
                    }
                }
            }
            if(count < max) {
                break;
            }
            oldest.delete();
        }
        SimpleDateFormat sd = new SimpleDateFormat("yyMMdd-HHmmss");
        String date = sd.format(new Date());
        File zipFile = new File(root, "backup-" + date + ".zip");
        ArrayList list = new ArrayList();
        File base = new File(sourcePath);
        listRecursive(list, base);
        if(list.size() == 0) {
            FileOutputStream out = new FileOutputStream(zipFile);
            out.close();
        } else {
            OutputStream out = null;
            try {
                out = new FileOutputStream(zipFile);
                ZipOutputStream zipOut = new ZipOutputStream(out);
                String baseName = base.getAbsolutePath();
                for(int i=0; i<list.size(); i++) {
                    File f = (File) list.get(i);
                    String fileName = f.getAbsolutePath();
                    String entryName = fileName;
                    if(fileName.startsWith(baseName)) {
                        entryName = entryName.substring(baseName.length());
                    }
                    if(entryName.startsWith("\\")) {
                        entryName = entryName.substring(1);
                    }
                    if(!entryName.startsWith("/")) {
                        entryName = "/" + entryName;
                    }
                    ZipEntry entry = new ZipEntry(basePath + entryName);
                    zipOut.putNextEntry(entry);
                    InputStream in = null;
                    try {
                        in = new FileInputStream(fileName);
                        IOUtils.copyAndCloseInput(in, zipOut);
                    } finally {
                    	IOUtils.closeSilently(in);
                    }
                    zipOut.closeEntry();
                }
                zipOut.closeEntry();
                zipOut.close();
            } finally {
                IOUtils.closeSilently(out);
            }
        }
        return zipFile;
    }    
    
    static void listRecursive(List list, File file) throws IOException {
        File[] l = file.listFiles();
        for(int i=0; l != null && i<l.length; i++) {
            File f = l[i];
            if(f.isDirectory()) {
                listRecursive(list, f);
            } else {
                list.add(f);
            }
        }
    }    

    static void deleteRecursive(File file) throws IOException {
        if(file.isDirectory()) {
            File[] list = file.listFiles();
            for(int i=0; i<list.length; i++) {
                deleteRecursive(list[i]);
            }
        } 
        if(file.exists() && !file.delete()) {
            throw new IOException("Could not delete " + file.getAbsolutePath());
        }
    }    
    
    private void testLoop() throws Exception {
        random = new SecureRandom();
        while(true) {
            runOneTest(random.nextInt());
        }
    }
    
    Connection openConnection() throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:" + TEST_DIRECTORY + "/test", "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE IF NOT EXISTS TEST(ID IDENTITY, NAME VARCHAR)");
        return conn;
    }
    
    private void runOneTest(int i) throws Exception {
        Random random = new Random(i);
        Connection conn = openConnection();
    	PreparedStatement prep = null;
        while (true) {
            boolean rollback = random.nextInt(10) == 1;
            int len;
            if (random.nextInt(10) == 1) {
                len = random.nextInt(8000) * 2;
            } else {
                len = random.nextInt(2) * 2;
            }
            if (rollback && random.nextBoolean()) {
                // make the length odd
                len++;
            }
            byte[] data = new byte[len];
            random.nextBytes(data);
            int op = random.nextInt();
            if (op % 100 == 0) {
            	conn.close();
            	conn = openConnection();
            	prep = null;
            }
        	if(prep == null) {
            	prep = conn.prepareStatement("INSERT INTO TEST(NAME) VALUES(?)");
            	conn.setAutoCommit(false);
        	}            
            prep.setString(1, "" + len);
            prep.execute();
            if (rollback) {
            	conn.rollback();
            } else {
            	conn.commit();
            }
        }
    }

    private boolean testConsistency() {
        FileOutputStream out = null;
        PrintWriter p = null;
        try {
            out = new FileOutputStream(TEST_DIRECTORY + "/result.txt");
            p = new PrintWriter(out);
            p.println("Results");
            p.flush();
        } catch(Throwable t) {
            t.printStackTrace();
            System.exit(0);
        }
        Connection conn = null;
        try {
            conn = openConnection();
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
            while(rs.next()) {
            	String name = rs.getString("NAME");
            	int value = Integer.parseInt(name);
            	if(value % 2 == 1) {
            		throw new Exception("unexpected odd entry " + rs.getInt("ID"));
            	}
            }
            conn.close();
            return true;
        } catch(Throwable t) {
            t.printStackTrace();
            t.printStackTrace(p);
            return false;
        } finally {
            if(conn != null) {
                try {
                    conn.close();
                } catch(Throwable t2) {
                    t2.printStackTrace();
                    t2.printStackTrace(p);
                }
            }
            p.flush();
            p.close();
            IOUtils.closeSilently(out);
        }
    }
    
}
