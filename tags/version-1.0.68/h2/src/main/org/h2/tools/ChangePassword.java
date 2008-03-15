/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

import org.h2.engine.Database;
import org.h2.message.Message;
import org.h2.security.SHA256;
import org.h2.store.FileLister;
import org.h2.store.FileStore;
import org.h2.util.FileUtils;

/**
 * A tools to change, remove or set a file password of a database without
 * opening it.
 */
public class ChangePassword {

    private String dir;
    private String cipher;
    private byte[] decrypt;
    private byte[] encrypt;

    // TODO security: maybe allow functions in the url
    // jdbc:h2:test;action=[decrypt|encrypt|check|reindex|recover|compress...]
    // and/or implement SQL commands that call this functions (only for the admin)

    private void showUsage() {
        System.out.println("java "+getClass().getName()
                + " [-dir <dir>] [-db <database>] [-cipher <cipher>] [-decrypt <pwd>] [-encrypt <pwd>] [-quiet]");
        System.out.println("See also http://h2database.com/javadoc/org/h2/tools/ChangePassword.html");
    }

    /**
     * The command line interface for this tool.
     * The options must be split into strings like this: "-db", "test",...
     * Options are case sensitive. The following options are supported:
     * <ul>
     * <li>-help or -? (print the list of options)
     * </li><li>-dir database directory (the default is the current directory)
     * </li><li>-db database name (all databases if no name is specified)
     * </li><li>-cipher type (AES or XTEA)
     * </li><li>-decrypt password (null if the database is not encrypted)
     * </li><li>-encrypt password (null if the database should not be encrypted)
     * </li><li>-quiet does not print progress information
     * </li></ul>
     *
     * @param args the command line arguments
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        new ChangePassword().run(args);
    }

    private void run(String[] args) throws SQLException {
        String dir = ".";
        String cipher = null;
        char[] decryptPassword = null;
        char[] encryptPassword = null;
        String db = null;
        boolean quiet = false;
        for (int i = 0; args != null && i < args.length; i++) {
            if (args[i].equals("-dir")) {
                dir = args[++i];
            } else if (args[i].equals("-cipher")) {
                cipher = args[++i];
            } else if (args[i].equals("-db")) {
                db = args[++i];
            } else if (args[i].equals("-decrypt")) {
                decryptPassword = args[++i].toCharArray();
            } else if (args[i].equals("-encrypt")) {
                encryptPassword = args[++i].toCharArray();
            } else if (args[i].equals("-quiet")) {
                quiet = true;
            } else {
                showUsage();
                return;
            }
        }
        if (encryptPassword == null && decryptPassword == null) {
            showUsage();
            return;
        }
        execute(dir, db, cipher, decryptPassword, encryptPassword, quiet);
    }

    /**
     * Get the file encryption key for a given password.
     * The password must be supplied as char arrays and is cleaned in this method.
     *
     * @param password the password as a char array
     * @return the encryption key
     */
    private static byte[] getFileEncryptionKey(char[] password) {
        if (password == null) {
            return null;
        }
        SHA256 sha = new SHA256();
        return sha.getKeyPasswordHash("file", password);
    }

    /**
     * Changes the password for a database.
     * The passwords must be supplied as char arrays and are cleaned in this method.
     *
     * @param dir the directory (. for the current directory)
     * @param db the database name (null for all databases)
     * @param cipher the cipher (AES, XTEA)
     * @param decryptPassword the decryption password as a char array
     * @param encryptPassword the encryption password as a char array
     * @param quiet don't print progress information
     * @throws SQLException
     */
    public static void execute(String dir, String db, String cipher, char[] decryptPassword, char[] encryptPassword, boolean quiet) throws SQLException {
        ChangePassword change = new ChangePassword();
        change.dir = dir;
        change.cipher = cipher;
        change.decrypt = getFileEncryptionKey(decryptPassword);
        change.encrypt = getFileEncryptionKey(encryptPassword);

        // first, test only if the file can be renamed 
        // (to find errors with locked files early)
        ArrayList files = FileLister.getDatabaseFiles(dir, db, false);
        for (int i = 0; i < files.size(); i++) {
            String fileName = (String) files.get(i);
            String temp = dir + "/temp.db";
            FileUtils.delete(temp);
            FileUtils.rename(fileName, temp);
            FileUtils.rename(temp, fileName);
        }
        // if this worked, the operation will (hopefully) be successful
        // TODO changePassword: this is a workaround! 
        // make the operation atomic (all files or none)
        for (int i = 0; i < files.size(); i++) {
            String fileName = (String) files.get(i);
            change.process(fileName);
        }
        if (files.size() == 0 && !quiet) {
            System.out.println("No database files found");
        }
    }

    private void process(String fileName) throws SQLException {
        boolean textStorage = Database.isTextStorage(fileName, false);
        byte[] magic = Database.getMagic(textStorage);
        FileStore in;
        if (decrypt == null) {
            in = FileStore.open(null, fileName, "r", magic);
        } else {
            in = FileStore.open(null, fileName, "r", magic, cipher, decrypt);
        }
        in.init();
        copy(fileName, textStorage, in, encrypt);
    }

    private void copy(String fileName, boolean textStorage, FileStore in, byte[] key) throws SQLException {
        String temp = dir + "/temp.db";
        FileUtils.delete(temp);
        byte[] magic = Database.getMagic(textStorage);
        FileStore out;
        if (key == null) {
            out = FileStore.open(null, temp, "rw", magic);
        } else {
            out = FileStore.open(null, temp, "rw", magic, cipher, key);
        }
        out.init();
        byte[] buffer = new byte[4 * 1024];
        long remaining = in.length() - FileStore.HEADER_LENGTH;
        long total = remaining;
        in.seek(FileStore.HEADER_LENGTH);
        out.seek(FileStore.HEADER_LENGTH);
        long time = System.currentTimeMillis();
        while (remaining > 0) {
            if (System.currentTimeMillis() - time > 1000) {
                System.out.println(fileName + ": " + (100 - 100 * remaining / total) + "%");
                time = System.currentTimeMillis();
            }
            int len = (int) Math.min(buffer.length, remaining);
            in.readFully(buffer, 0, len);
            out.write(buffer, 0, len);
            remaining -= len;
        }
        try {
            in.close();
            out.close();
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
        FileUtils.delete(fileName);
        FileUtils.rename(temp, fileName);
    }

}
