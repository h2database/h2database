/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.text.CollationKey;
import java.text.Collator;
import java.util.Locale;

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

    private final Collator collator;
    private final String name;
    private final SmallLRUCache collationKeys;

    /**
     * Create a new compare mode with the given collator and cache size.
     * The cache is used to speed up comparison when using a collator;
     * CollationKey objects are cached.
     *
     * @param collator the collator or null
     * @param name the collation name or null
     * @param cacheSize the number of entries in the CollationKey cache
     */
    public CompareMode(Collator collator, String name, int cacheSize) {
        this.collator = collator;
        if (collator != null && cacheSize != 0) {
            collationKeys = new SmallLRUCache(cacheSize);
        } else {
            collationKeys = null;
        }
        this.name = name == null ? OFF : name;
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
            CollationKey key = (CollationKey) collationKeys.get(a);
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
            Locale[] locales = Collator.getAvailableLocales();
            for (int i = 0; i < locales.length; i++) {
                Locale locale = locales[i];
                if (compareLocaleNames(locale, name)) {
                    result = Collator.getInstance(locale);
                    break;
                }
            }
        }
        return result;
    }

    public String getName() {
        return name;
    }

}
