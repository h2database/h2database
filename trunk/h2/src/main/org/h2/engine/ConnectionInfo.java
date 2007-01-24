/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

import org.h2.command.dml.SetTypes;
import org.h2.message.Message;
import org.h2.security.SHA256;
import org.h2.util.FileUtils;
import org.h2.util.MathUtils;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;

public class ConnectionInfo {

    private String originalURL;
    private String url;
    private Properties prop = new Properties();
    private String user;
    private byte[] filePasswordHash;
    private byte[] userPasswordHash;
    private String name;

    private static HashSet KNOWN_SETTINGS = new HashSet();

    private boolean remote;
    private boolean ssl;
    private boolean persistent;

    static {
        ObjectArray list = SetTypes.getSettings();
        for(int i=0; i<list.size(); i++) {
            KNOWN_SETTINGS.add(list.get(i));
        }
        // TODO document these settings
        String[] connectionTime = new String[]{
                "PASSWORD", "USER", "STORAGE", "FILE_LOCK", "CIPHER", "DB_CLOSE_ON_EXIT",
                "IGNORE_UNKNOWN_SETTINGS", "IFEXISTS", "RECOVER", "CREATE", "CACHE_TYPE"
        };
        for(int i=0; i<connectionTime.length; i++) {
            String key = connectionTime[i];
            if(Constants.CHECK && KNOWN_SETTINGS.contains(key)) {
                throw Message.getInternalError(key);
            }
            KNOWN_SETTINGS.add(key);
        }
    }

    public ConnectionInfo(String name) {
        this.name = name;
        parseName();
    }

    public ConnectionInfo(String u, Properties info) throws SQLException {
        this.originalURL = u;
        if(!u.startsWith(Constants.START_URL)) {
            throw Message.getInvalidValueException(u, "url");
        }
        this.url = u;
        readProperties(info);
        readSettings();
        readUser();
        readPasswords();
        name = url.substring(Constants.START_URL.length());
        parseName();
    }

    private void parseName() {
        if(name.equals(".")) {
            name = "mem:";
        }
        if(name.startsWith("tcp:")) {
            remote = true;
            name = name.substring("tcp:".length());
        } else if(name.startsWith("ssl:")) {
            remote = true;
            ssl = true;
            name = name.substring("ssl:".length());
        } else if(name.startsWith("mem:")) {
            persistent = false;
        } else if(name.startsWith("file:")) {
            name = name.substring("file:".length());
            persistent = true;
        } else {
            persistent = true;
        }
    }

    public String getDatabaseName() {
        if(remote) {
            if(ssl) {
                return "ssl:" + name;
            } else {
                return "tcp:" + name;
            }
        } else if(persistent) {
            return "file:" + name;
        }
        return name;
    }

    public void setBaseDir(String dir) {
        if(persistent) {
            name = dir + System.getProperty("file.separator") + name;
        }
    }

    public boolean isRemote() {
        return remote;
    }

    public boolean isPersistent() {
        return persistent;
    }

    private void readProperties(Properties info) throws SQLException {
        Object[] list = new Object[info.size()];
        info.keySet().toArray(list);
        for(int i=0; i<list.length; i++) {
            String key = StringUtils.toUpperEnglish(list[i].toString());
            if(prop.containsKey(key)) {
                throw Message.getSQLException(Message.DUPLICATE_PROPERTY_1, key);
            }
            if(KNOWN_SETTINGS.contains(key)) {
                prop.put(key, info.get(list[i]));
            }
        }
    }

    private void readSettings() throws SQLException {
        int idx = url.indexOf(';');
        if(idx >= 0) {
            String settings = url.substring(idx + 1);
            url = url.substring(0, idx);
            String[] list = StringUtils.arraySplit(settings, ';', false);
            for(int i=0; i<list.length; i++) {
                String setting = list[i];
                int equal = setting.indexOf('=');
                if(equal < 0) {
                    throw getFormatException();
                }
                String value = setting.substring(equal+1);
                String key = setting.substring(0, equal);
                key = StringUtils.toUpperEnglish(key);
                if(!KNOWN_SETTINGS.contains(key)) {
                    throw Message.getSQLException(Message.UNSUPPORTED_SETTING_1, key);
                }
                String old = prop.getProperty(key);
                if(old != null && !old.equals(value)) {
                    throw Message.getSQLException(Message.DUPLICATE_PROPERTY_1, key);
                }
                prop.setProperty(key, value);
            }
        }
    }

    private char[] removePassword() {
        Object p = prop.remove("PASSWORD");
        if(p == null) {
            return new char[0];
        } else if(p instanceof char[]) {
            return (char[])p;
        } else {
            return p.toString().toCharArray();
        }
    }

    private void readUser() {
        // TODO document: the user name is case-insensitive (stored uppercase) and english conversion is used
        user = StringUtils.toUpperEnglish(removeProperty("USER", ""));
    }

    void readPasswords() throws SQLException {
        char[] password = removePassword();
        SHA256 sha = new SHA256();
        if(getProperty("CIPHER", null) != null) {
            // split password into (filePassword+' '+userPassword)
            int space = -1;
            for(int i=0; i<password.length;i++) {
                if(password[i] == ' ') {
                    space = i;
                    break;
                }
            }
            if(space < 0) {
                throw Message.getSQLException(Message.WRONG_PASSWORD_FORMAT);
            }
            char[] np = new char[password.length - space -1];
            char[] filePassword = new char[space];
            System.arraycopy(password, space+1, np, 0, np.length);
            System.arraycopy(password, 0, filePassword, 0, space);
            Arrays.fill(password, (char)0);
            password = np;
            filePasswordHash = sha.getKeyPasswordHash("file", filePassword);
        }
        userPasswordHash = sha.getKeyPasswordHash(user, password);
    }

    public boolean removeProperty(String key, boolean defaultValue) {
        String x = removeProperty(key, null);
        return x == null ? defaultValue : Boolean.valueOf(x).booleanValue();
    }

    public String removeProperty(String key, String defaultValue) {
        if(Constants.CHECK && !KNOWN_SETTINGS.contains(key)) {
            throw Message.getInternalError(key);
        }
        Object x = prop.remove(key);
        return x == null ? defaultValue : x.toString();
    }

    public String getName() throws SQLException {
        if(persistent) {
            String n = FileUtils.normalize(name + Constants.SUFFIX_DATA_FILE);
            n = n.substring(0, n.length() - Constants.SUFFIX_DATA_FILE.length());
            return FileUtils.normalize(n);
        }
        return name;
    }

    public byte[] getFilePasswordHash() {
        return filePasswordHash;
    }

    public String getUserName() {
        return user;
    }

    public byte[] getUserPasswordHash() {
        return userPasswordHash;
    }

    public String[] getKeys() {
        String[] keys = new String[prop.size()];
        prop.keySet().toArray(keys);
        return keys;
    }

    public String getProperty(String key) {
        return prop.getProperty(key);
    }

    public String getProperty(String key, String defaultValue) {
        if(Constants.CHECK && !KNOWN_SETTINGS.contains(key)) {
            throw Message.getInternalError(key);
        }
        String s = getProperty(key);
        return s == null ? defaultValue : s;
    }

    public String getProperty(int setting, String defaultValue) {
        String key = SetTypes.getTypeName(setting);
        String s = getProperty(key);
        return s == null ? defaultValue : s;
    }

    public int getIntProperty(int setting, int defaultValue) {
        String key = SetTypes.getTypeName(setting);
        String s = getProperty(key, null);
        try {
            return s == null ? defaultValue : MathUtils.decodeInt(s);
        } catch(NumberFormatException  e) {
            return defaultValue;
        }
    }

    public boolean isSSL() {
        return ssl;
    }

    public void setUserName(String name) {
        this.user = name;
    }

    public void setUserPasswordHash(byte[] bs) {
        this.userPasswordHash = bs;
    }

    public void setFilePasswordHash(byte[] bs) {
        this.filePasswordHash = bs;
    }

    public void setProperty(String key, String value) {
        prop.setProperty(key, value);
    }

    public String getURL() {
        return url;
    }

    public String getOriginalURL() {
        return originalURL;
    }

    public void setOriginalURL(String url) {
        originalURL = url;
    }

    boolean getTextStorage() throws SQLException {
        String storage = removeProperty("STORAGE", "BINARY");
        if("BINARY".equalsIgnoreCase(storage)) {
            return false;
        } else if("TEXT".equalsIgnoreCase(storage)) {
            return true;
        } else {
            throw Message.getInvalidValueException(storage, "storage");
        }
    }

    public SQLException getFormatException() {
        String format = Constants.URL_FORMAT;
        return Message.getSQLException(Message.URL_FORMAT_ERROR_2, new String[]{format, url}, null);
    }

}
