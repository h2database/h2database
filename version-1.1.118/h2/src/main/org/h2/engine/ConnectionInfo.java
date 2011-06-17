/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import org.h2.api.DatabaseEventListener;
import org.h2.command.dml.SetTypes;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.message.Message;
import org.h2.security.SHA256;
import org.h2.util.ByteUtils;
import org.h2.util.FileUtils;
import org.h2.util.MathUtils;
import org.h2.util.New;
import org.h2.util.StringUtils;

/**
 * Encapsulates the connection settings, including user name and password.
 */
public class ConnectionInfo implements Cloneable {
    private static final HashSet<String> KNOWN_SETTINGS = New.hashSet();
    private Properties prop = new Properties();
    private String originalURL;
    private String url;
    private String user;
    private byte[] filePasswordHash;
    private byte[] userPasswordHash;

    /**
     * The database name
     */
    private String name;
    private boolean remote;
    private boolean ssl;
    private boolean persistent;
    private boolean unnamed;

    /**
     * Create a connection info object.
     *
     * @param name the database name (including tags)
     */
    public ConnectionInfo(String name) {
        this.name = name;
        parseName();
    }

    /**
     * Create a connection info object.
     *
     * @param u the database URL (must start with jdbc:h2:)
     * @param info the connection properties
     */
    public ConnectionInfo(String u, Properties info) throws SQLException {
        this.originalURL = u;
        if (!u.startsWith(Constants.START_URL)) {
            throw Message.getInvalidValueException(u, "url");
        }
        this.url = u;
        readProperties(info);
        readSettingsFromURL();
        setUserName(removeProperty("USER", ""));
        convertPasswords();
        name = url.substring(Constants.START_URL.length());
        parseName();
    }

    static {
        ArrayList<String> list = SetTypes.getTypes();
        HashSet<String> set = KNOWN_SETTINGS;
        set.addAll(list);
        // TODO document these settings
        String[] connectionTime = new String[] { "ACCESS_MODE_LOG", "ACCESS_MODE_DATA", "AUTOCOMMIT", "CIPHER",
                "CREATE", "CACHE_TYPE", "DB_CLOSE_ON_EXIT", "FILE_LOCK", "IGNORE_UNKNOWN_SETTINGS", "IFEXISTS",
                "PASSWORD", "RECOVER", "USER", "DATABASE_EVENT_LISTENER_OBJECT", "AUTO_SERVER",
                "AUTO_RECONNECT", "OPEN_NEW", "PAGE_STORE" };
        for (String key : connectionTime) {
            if (SysProperties.CHECK && set.contains(key)) {
                Message.throwInternalError(key);
            }
            set.add(key);
        }
    }

    private static boolean isKnownSetting(String s) {
        return KNOWN_SETTINGS.contains(s);
    }

    public Object clone() throws CloneNotSupportedException {
        ConnectionInfo clone = (ConnectionInfo) super.clone();
        clone.prop = (Properties) prop.clone();
        clone.filePasswordHash = ByteUtils.cloneByteArray(filePasswordHash);
        clone.userPasswordHash = ByteUtils.cloneByteArray(userPasswordHash);
        return clone;
    }

    private void parseName() {
        if (".".equals(name)) {
            name = "mem:";
        }
        if (name.startsWith("tcp:")) {
            remote = true;
            name = name.substring("tcp:".length());
        } else if (name.startsWith("ssl:")) {
            remote = true;
            ssl = true;
            name = name.substring("ssl:".length());
        } else if (name.startsWith("mem:")) {
            persistent = false;
            if ("mem:".equals(name)) {
                unnamed = true;
            }
        } else if (name.startsWith("file:")) {
            name = name.substring("file:".length());
            persistent = true;
        } else {
            persistent = true;
        }
    }

    /**
     * Set the base directory of persistent databases, unless the database is in
     * the user home folder (~).
     *
     * @param dir the new base directory
     */
    public void setBaseDir(String dir) {
        if (persistent) {
            if (!name.startsWith("~")) {
                name = dir + SysProperties.FILE_SEPARATOR + name;
            }
        }
    }

    /**
     * Check if this is a remote connection.
     *
     * @return true if it is
     */
    public boolean isRemote() {
        return remote;
    }

    /**
     * Check if the referenced database is persistent.
     *
     * @return true if it is
     */
    boolean isPersistent() {
        return persistent;
    }

    /**
     * Check if the referenced database is an unnamed in-memory database.
     *
     * @return true if it is
     */
    boolean isUnnamedInMemory() {
        return unnamed;
    }

    private void readProperties(Properties info) throws SQLException {
        Object[] list = new Object[info.size()];
        info.keySet().toArray(list);
        for (Object k : list) {
            String key = StringUtils.toUpperEnglish(k.toString());
            if (prop.containsKey(key)) {
                throw Message.getSQLException(ErrorCode.DUPLICATE_PROPERTY_1, key);
            }
            if (isKnownSetting(key)) {
                prop.put(key, info.get(k));
            }
        }
    }

    private void readSettingsFromURL() throws SQLException {
        int idx = url.indexOf(';');
        if (idx >= 0) {
            String settings = url.substring(idx + 1);
            url = url.substring(0, idx);
            String[] list = StringUtils.arraySplit(settings, ';', false);
            for (String setting : list) {
                int equal = setting.indexOf('=');
                if (equal < 0) {
                    throw getFormatException();
                }
                String value = setting.substring(equal + 1);
                String key = setting.substring(0, equal);
                key = StringUtils.toUpperEnglish(key);
                if (!isKnownSetting(key)) {
                    throw Message.getSQLException(ErrorCode.UNSUPPORTED_SETTING_1, key);
                }
                String old = prop.getProperty(key);
                if (old != null && !old.equals(value)) {
                    throw Message.getSQLException(ErrorCode.DUPLICATE_PROPERTY_1, key);
                }
                prop.setProperty(key, value);
            }
        }
    }

    /**
     * Removes the database event listener object.
     */
    void removeDatabaseEventListenerObject() {
        prop.remove("DATABASE_EVENT_LISTENER_OBJECT");
    }

    /**
     * Return the database event listener object set as a Java object. If the
     * event listener is not set or set as a string (the class name), then this
     * method returns null.
     *
     * @return the database event listener object or null
     */
    DatabaseEventListener getDatabaseEventListenerObject() throws SQLException {
        Object p = prop.get("DATABASE_EVENT_LISTENER_OBJECT");
        if (p == null) {
            return null;
        }
        if (p instanceof DatabaseEventListener) {
            return (DatabaseEventListener) p;
        }
        throw Message.getSQLException(ErrorCode.DATA_CONVERSION_ERROR_1, p.getClass().getName());
    }

    private char[] removePassword() {
        Object p = prop.remove("PASSWORD");
        if (p == null) {
            return new char[0];
        } else if (p instanceof char[]) {
            return (char[]) p;
        } else {
            return p.toString().toCharArray();
        }
    }

    /**
     * Split the password property into file password and user password if
     * necessary, and convert them to the internal hash format.
     */
    public void convertPasswords() throws SQLException {
        char[] password = removePassword();
        SHA256 sha = new SHA256();
        if (getProperty("CIPHER", null) != null) {
            // split password into (filePassword+' '+userPassword)
            int space = -1;
            for (int i = 0; i < password.length; i++) {
                if (password[i] == ' ') {
                    space = i;
                    break;
                }
            }
            if (space < 0) {
                throw Message.getSQLException(ErrorCode.WRONG_PASSWORD_FORMAT);
            }
            char[] np = new char[password.length - space - 1];
            char[] filePassword = new char[space];
            System.arraycopy(password, space + 1, np, 0, np.length);
            System.arraycopy(password, 0, filePassword, 0, space);
            Arrays.fill(password, (char) 0);
            password = np;
            filePasswordHash = sha.getKeyPasswordHash("file", filePassword);
        }
        userPasswordHash = sha.getKeyPasswordHash(user, password);
    }

    /**
     * Get a boolean property if it is set and return the value.
     *
     * @param key the property name
     * @param defaultValue the default value
     * @return the value
     */
    public boolean getProperty(String key, boolean defaultValue) {
        String x = getProperty(key, null);
        if (x == null) {
            return defaultValue;
        }
        // support 0 / 1 (like the parser)
        if (x.length() == 1 && Character.isDigit(x.charAt(0))) {
            return Integer.parseInt(x) != 0;
        }
        return Boolean.valueOf(x).booleanValue();
    }

    /**
     * Remove a boolean property if it is set and return the value.
     *
     * @param key the property name
     * @param defaultValue the default value
     * @return the value
     */
    public boolean removeProperty(String key, boolean defaultValue) {
        String x = removeProperty(key, null);
        return x == null ? defaultValue : Boolean.valueOf(x).booleanValue();
    }

    /**
     * Remove a String property if it is set and return the value.
     *
     * @param key the property name
     * @param defaultValue the default value
     * @return the value
     */
    String removeProperty(String key, String defaultValue) {
        if (SysProperties.CHECK && !isKnownSetting(key)) {
            Message.throwInternalError(key);
        }
        Object x = prop.remove(key);
        return x == null ? defaultValue : x.toString();
    }

    /**
     * Get the unique and normalized database name (excluding settings).
     *
     * @return the database name
     */
    String getName() throws SQLException {
        if (persistent) {
            String suffix;
            if (SysProperties.getPageStore()) {
                suffix = Constants.SUFFIX_PAGE_FILE;
            } else {
                suffix = Constants.SUFFIX_DATA_FILE;
            }
            String n = FileUtils.normalize(name + suffix);
            String fileName = FileUtils.getFileName(n);
            if (fileName.length() < suffix.length() + 1) {
                throw Message.getSQLException(ErrorCode.INVALID_DATABASE_NAME_1, name);
            }
            n = n.substring(0, n.length() - suffix.length());
            return FileUtils.normalize(n);
        }
        return name;
    }

    /**
     * Get the file password hash if it is set.
     *
     * @return the password hash or null
     */
    byte[] getFilePasswordHash() {
        return filePasswordHash;
    }

    /**
     * Get the name of the user.
     *
     * @return the user name
     */
    public String getUserName() {
        return user;
    }

    /**
     * Get the user password hash.
     *
     * @return the password hash
     */
    byte[] getUserPasswordHash() {
        return userPasswordHash;
    }

    /**
     * Get the property keys.
     *
     * @return the property keys
     */
    String[] getKeys() {
        String[] keys = new String[prop.size()];
        prop.keySet().toArray(keys);
        return keys;
    }

    /**
     * Get the value of the given property.
     *
     * @param key the property key
     * @return the value as a String
     */
    String getProperty(String key) {
        Object value = prop.get(key);
        if (value == null || !(value instanceof String)) {
            return null;
        }
        return value.toString();
    }

    /**
     * Get the value of the given property.
     *
     * @param key the property key
     * @param defaultValue the default value
     * @return the value as a String
     */
    public String getProperty(String key, String defaultValue) {
        if (SysProperties.CHECK && !isKnownSetting(key)) {
            Message.throwInternalError(key);
        }
        String s = getProperty(key);
        return s == null ? defaultValue : s;
    }

    /**
     * Get the value of the given property.
     *
     * @param setting the setting id
     * @param defaultValue the default value
     * @return the value as a String
     */
    String getProperty(int setting, String defaultValue) {
        String key = SetTypes.getTypeName(setting);
        String s = getProperty(key);
        return s == null ? defaultValue : s;
    }

    /**
     * Get the value of the given property.
     *
     * @param setting the setting id
     * @param defaultValue the default value
     * @return the value as an integer
     */
    int getIntProperty(int setting, int defaultValue) {
        String key = SetTypes.getTypeName(setting);
        String s = getProperty(key, null);
        try {
            return s == null ? defaultValue : MathUtils.decodeInt(s);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Check if this is a remote connection with SSL enabled.
     *
     * @return true if it is
     */
    boolean isSSL() {
        return ssl;
    }

    /**
     * Overwrite the user name. The user name is case-insensitive and stored in
     * uppercase. English conversion is used.
     *
     * @param name the user name
     */
    public void setUserName(String name) {
        this.user = StringUtils.toUpperEnglish(name);
    }

    /**
     * Set the user password hash.
     *
     * @param hash the new hash value
     */
    public void setUserPasswordHash(byte[] hash) {
        this.userPasswordHash = hash;
    }

    /**
     * Set the file password hash.
     *
     * @param hash the new hash value
     */
    public void setFilePasswordHash(byte[] hash) {
        this.filePasswordHash = hash;
    }

    /**
     * Overwrite a property.
     *
     * @param key the property name
     * @param value the value
     */
    public void setProperty(String key, String value) {
        // value is null if the value is an object
        if (value != null) {
            prop.setProperty(key, value);
        }
    }

    /**
     * Get the database URL.
     *
     * @return the URL
     */
    public String getURL() {
        return url;
    }

    /**
     * Get the complete original database URL.
     *
     * @return the database URL
     */
    public String getOriginalURL() {
        return originalURL;
    }

    /**
     * Set the original database URL.
     *
     * @param url the database url
     */
    public void setOriginalURL(String url) {
        originalURL = url;
    }

    /**
     * Generate an URL format exception.
     *
     * @return the exception
     */
    SQLException getFormatException() {
        String format = Constants.URL_FORMAT;
        return Message.getSQLException(ErrorCode.URL_FORMAT_ERROR_2, format, url);
    }

    /**
     * Switch to server mode, and set the server name and database key.
     *
     * @param serverKey the server name, '/', and the security key
     */
    public void setServerKey(String serverKey) {
        remote = true;
        persistent = false;
        this.name = serverKey;
    }

}
