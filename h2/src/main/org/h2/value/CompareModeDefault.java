/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.text.CollationKey;
import java.text.Collator;

import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.util.SmallLRUCache;

/**
 * The default implementation of CompareMode. It uses java.text.Collator.
 */
public class CompareModeDefault extends CompareMode {

    private final Collator collator;
    private final SmallLRUCache<String, CollationKey> collationKeys;

    private volatile CompareModeDefault caseInsensitive;

    protected CompareModeDefault(String name, int strength) {
        super(name, strength);
        collator = CompareMode.getCollator(name);
        if (collator == null) {
            throw DbException.getInternalError(name);
        }
        collator.setStrength(strength);
        int cacheSize = SysProperties.COLLATOR_CACHE_SIZE;
        if (cacheSize != 0) {
            collationKeys = SmallLRUCache.newInstance(cacheSize);
        } else {
            collationKeys = null;
        }
    }

    @Override
    public int compareString(String a, String b, boolean ignoreCase) {
        if (ignoreCase && getStrength() > Collator.SECONDARY) {
            CompareModeDefault i = caseInsensitive;
            if (i == null) {
                caseInsensitive = i = new CompareModeDefault(getName(), Collator.SECONDARY);
            }
            return i.compareString(a, b, false);
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

    @Override
    public boolean equalsChars(String a, int ai, String b, int bi,
            boolean ignoreCase) {
        return compareString(a.substring(ai, ai + 1), b.substring(bi, bi + 1),
                ignoreCase) == 0;
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

}
