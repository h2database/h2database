/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.text.Collator;
import java.util.Locale;

import org.h2.util.StringUtils;

/**
 * Instances of this class can compare strings.
 * Case sensitive and case insensitive comparison is supported,
 * and comparison using a collator.
 */
public class CompareMode {
    public static final String OFF = "OFF";
    private final Collator collator;
    private final String name;

    public CompareMode(Collator collator, String name) {
        this.collator = collator;
        this.name = name == null ? OFF : name;
    }

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
        int comp = collator.compare(a, b);
        return comp;
    }

    public static String getName(Locale l) {
        Locale english = Locale.ENGLISH;
        String name = l.getDisplayLanguage(english) + ' ' + l.getDisplayCountry(english) + ' ' + l.getVariant();
        name = StringUtils.toUpperEnglish(name.trim().replace(' ', '_'));
        return name;
    }

    private static boolean compareLocaleNames(Locale locale, String name) {
        return name.equalsIgnoreCase(locale.toString()) || name.equalsIgnoreCase(getName(locale));
    }

    public static Collator getCollator(String name) {
        Collator result = null;
        if (name.length() == 2) {
            Locale locale = new Locale(name.toLowerCase(), "");
            if (compareLocaleNames(locale, name)) {
                result = Collator.getInstance(locale);
            }
        } else if (name.length() == 5) {
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
