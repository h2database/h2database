/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.text.Collator;
import java.util.Locale;

import org.h2.util.StringUtils;

public class CompareMode {
    public static final String OFF = "OFF";
    private String name = OFF;
    private Collator collator;
    
    public CompareMode(Collator collator, String name) {
        this.collator = collator;
        if(name != null) {
            this.name = name;
        }
    }
    
    public int compareString(String a, String b, boolean ignoreCase) {
        if(collator == null) {
            if(ignoreCase) {
                return a.compareToIgnoreCase(b); 
            }
            return a.compareTo(b);
        }
        if(ignoreCase) {
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

    public static Collator getCollator(String name) {
        Locale[] locales = Collator.getAvailableLocales();
        for(int i=0; i<locales.length; i++) {
            Locale locale = locales[i];
            if(name.equalsIgnoreCase(locale.toString()) || name.equalsIgnoreCase(getName(locale))) {
                return Collator.getInstance(locale);
            }
        }
        return null;
    }

    public String getName() {
        return name;
    }
    
}
