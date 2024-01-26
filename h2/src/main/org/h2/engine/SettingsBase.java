/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.Utils;

/**
 * The base class for settings.
 */
public class SettingsBase {

    private final HashMap<String, String> settings;

    protected SettingsBase(HashMap<String, String> s) {
        this.settings = s;
    }

    /**
     * Get the setting for the given key.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the setting
     */
    protected boolean get(String key, boolean defaultValue) {
        String s = get(key, Boolean.toString(defaultValue));
        try {
            return Utils.parseBoolean(s, defaultValue, true);
        } catch (IllegalArgumentException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1,
                    e, "key:" + key + " value:" + s);
        }
    }

    /**
     * Set an entry in the key-value pair.
     *
     * @param key the key
     * @param value the value
     */
    void set(String key, boolean value) {
        settings.put(key, Boolean.toString(value));
    }

    /**
     * Get the setting for the given key.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the setting
     */
    protected int get(String key, int defaultValue) {
        String s = get(key, Integer.toString(defaultValue));
        try {
            return Integer.decode(s);
        } catch (NumberFormatException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1,
                    e, "key:" + key + " value:" + s);
        }
    }

    /**
     * Get the setting for the given key.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the setting
     */
    protected String get(String key, String defaultValue) {
        String v = settings.get(key);
        if (v != null) {
            return v;
        }
        StringBuilder buff = new StringBuilder("h2.");
        boolean nextUpper = false;
        for (int i = 0, l = key.length(); i < l; i++) {
            char c = key.charAt(i);
            if (c == '_') {
                nextUpper = true;
            } else {
                // Character.toUpperCase / toLowerCase ignores the locale
                buff.append(nextUpper ? Character.toUpperCase(c) : Character.toLowerCase(c));
                nextUpper = false;
            }
        }
        String sysProperty = buff.toString();
        v = Utils.getProperty(sysProperty, defaultValue);
        settings.put(key, v);
        return v;
    }

    /**
     * Check if the settings contains the given key.
     *
     * @param k the key
     * @return true if they do
     */
    protected boolean containsKey(String k) {
        return settings.containsKey(k);
    }

    /**
     * Get all settings.
     *
     * @return the settings
     */
    public HashMap<String, String> getSettings() {
        return settings;
    }

    /**
     * Get all settings in alphabetical order.
     *
     * @return the settings
     */
    public Entry<String, String>[] getSortedSettings() {
        @SuppressWarnings("unchecked")
        Map.Entry<String, String>[] entries = settings.entrySet().toArray(new Map.Entry[0]);
        Arrays.sort(entries, Comparator.comparing(Entry::getKey));
        return entries;
    }

}
