/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.Properties;
import org.h2.constant.ErrorCode;
import org.h2.message.DbException;

/**
 * The base class for settings.
 */
public class SettingsBase {

    private Properties properties;

    protected SettingsBase(Properties p) {
        this.properties = p;
    }

    protected boolean get(String key, boolean defaultValue) {
        String s = get(key, "" + defaultValue);
        try {
            return Boolean.valueOf(s).booleanValue();
        } catch (NumberFormatException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, "key:" + key + " value:" + s);
        }
    }

    protected int get(String key, int defaultValue) {
        String s = get(key, "" + defaultValue);
        try {
            return Integer.decode(s);
        } catch (NumberFormatException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, "key:" + key + " value:" + s);
        }
    }

    protected String get(String key, String defaultValue) {
        String keyUpper = key.toUpperCase();
        String v = properties.getProperty(keyUpper);
        if (v == null) {
            v = System.getProperty("h2." + key);
        }
        if (v == null) {
            properties.put(keyUpper, defaultValue);
            v = defaultValue;
        }
        return v;
    }

    public boolean containsKey(String k) {
        return properties.containsKey(k);
    }

}
