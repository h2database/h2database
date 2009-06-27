/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.text.CollationKey;
import java.text.Collator;
import java.util.Locale;

import org.h2.constant.SysProperties;
import org.h2.util.SmallLRUCache;
import org.h2.util.StringUtils;

/**
 * Instances of this class can compare strings.
 * Case sensitive and case insensitive comparison is supported,
 * and comparison using a collator.
 */
public class CompareMode {

    /**
     * This constant means there is no collator set,
     * and the default string comparison is to be used.
     */
    public static final String OFF = "OFF";

    private static CompareMode lastUsed;

    private final String name;
    private final int strength;
    private final Collator collator;
    private final SmallLRUCache<String, CollationKey> collationKeys;

    private CompareMode(String name, int strength) {
        this.name = name;
        this.strength = strength;
        this.collator = CompareMode.getCollator(name);
        int cacheSize = 0;
        if (collator != null) {
            this.collator.setStrength(strength);
            cacheSize = SysProperties.getCollatorCacheSize();
        }
        if (cacheSize != 0) {
            collationKeys = SmallLRUCache.newInstance(cacheSize);
        } else {
            collationKeys = null;
        }
    }

    /**
     * Create a new compare mode with the given collator and strength. If
     * required, a new CompareMode is created, or if possible the last one is
     * returned. A cache is used to speed up comparison when using a collator;
     * CollationKey objects are cached.
     *
     * @param name the collation name or null
     * @param strength the collation strength
     * @return the compare mode
     */
    public static CompareMode getInstance(String name, int strength) {
        if (lastUsed != null) {
            if (StringUtils.equals(lastUsed.name, name)) {
                if (lastUsed.strength == strength) {
                    return lastUsed;
                }
            }
        }
        lastUsed = new CompareMode(name, strength);
        return lastUsed;
    }

    /**
     * Compare two characters in a string.
     *
     * @param a the first string
     * @param ai the character index in the first string
     * @param b the second string
     * @param bi the character index in the second string
     * @param ignoreCase true if a case-insensitive comparison should be made
     * @return true if the characters are equals
     */
    public boolean equalsChars(String a, int ai, String b, int bi, boolean ignoreCase) {
        if (collator != null) {
            return compareString(a.substring(ai, ai + 1), b.substring(bi, bi + 1), ignoreCase) == 0;
        }
        char ca = a.charAt(ai);
        char cb = b.charAt(bi);
        if (ignoreCase) {
            ca = Character.toUpperCase(ca);
            cb = Character.toUpperCase(cb);
        }
        return ca == cb;
    }

    /**
     * Compare two strings.
     *
     * @param a the first string
     * @param b the second string
     * @param ignoreCase true if a case-insensitive comparison should be made
     * @return -1 if the first string is 'smaller', 1 if the second string is
     *         smaller, and 0 if they are equal
     */
    public int compareString(String a, String b, boolean ignoreCase) {
        if (collator == null) {
            if (ignoreCase) {
                return a.compareToIgnoreCase(b);
            }
            return a.compareTo(b);
        }
        if (ignoreCase) {
            // this is locale sensitive
            a = a.toUpperCase();
            b = b.toUpperCase();
        }
        int comp;
        if (collationKeys != null) {
            CollationKey aKey = getKey(a);
            CollationKey bKey = getKey(b);
            comp = aKey.compareTo(bKey);
        } else {
            comp = collator.compare(a, b);
        }
        return comp;
    }

    private CollationKey getKey(String a) {
        synchronized (collationKeys) {
            CollationKey key = collationKeys.get(a);
            if (key == null) {
                key = collator.getCollationKey(a);
                collationKeys.put(a, key);
            }
            return key;
        }
    }

    /**
     * Get the collation name.
     *
     * @param l the locale
     * @return the name of the collation
     */
    public static String getName(Locale l) {
        Locale english = Locale.ENGLISH;
        String name = l.getDisplayLanguage(english) + ' ' + l.getDisplayCountry(english) + ' ' + l.getVariant();
        name = StringUtils.toUpperEnglish(name.trim().replace(' ', '_'));
        return name;
    }

    private static boolean compareLocaleNames(Locale locale, String name) {
        return name.equalsIgnoreCase(locale.toString()) || name.equalsIgnoreCase(getName(locale));
    }

    /**
     * Get the collator object for the given language name or language / country
     * combination.
     *
     * @param name the language name
     * @return the collator
     */
    public static Collator getCollator(String name) {
        if (name == null || name.equals(OFF)) {
            return null;
        }
        Collator result = null;
        if (name.length() == 2) {
            Locale locale = new Locale(name.toLowerCase(), "");
            if (compareLocaleNames(locale, name)) {
                result = Collator.getInstance(locale);
            }
        } else if (name.length() == 5) {
            // LL_CC (language_country)
            int idx = name.indexOf('_');
            if (idx >= 0) {
                String language = name.substring(0, idx).toLowerCase();
                String country = name.substring(idx + 1);
                Locale locale = new Locale(language, country);
                if (compareLocaleNames(locale, name)) {
                    result = Collator.getInstance(locale);
                }
            }
        }
        if (result == null) {
            for (Locale locale : Collator.getAvailableLocales()) {
                if (compareLocaleNames(locale, name)) {
                    result = Collator.getInstance(locale);
                    break;
                }
            }
        }
        return result;
    }

    public String getName() {
        return name == null ? OFF : name;
    }

    public int getStrength() {
        return strength;
    }

}
