/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

/**
 * A cursor to iterate over all keys in new pages.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class ChangeCursor<K, V> extends Cursor<K, V> {

    private final long minVersion;

    ChangeCursor(MVMap<K, V> map, long minVersion) {
        super(map);
        this.minVersion = minVersion;
    }

    public CursorPos min(Page p, K from) {
        while (p != null && p.getVersion() >= minVersion) {
            if (p.isLeaf()) {
                CursorPos c = new CursorPos();
                c.page = p;
                c.index = 0;
                return c;
            }
            for (int i = 0; i < p.getChildPageCount(); i++) {
                if (isChildOld(p, i)) {
                    continue;
                }
                CursorPos c = new CursorPos();
                c.page = p;
                c.index = i;
                push(c);
                p = p.getChildPage(i);
                break;
            }
        }
        return null;
    }

    public CursorPos visitChild(Page p, int childIndex) {
        if (isChildOld(p, childIndex)) {
            return null;
        }
        return super.visitChild(p, childIndex);
    }

    private boolean isChildOld(Page p, int childIndex) {
        long pos = p.getChildPagePos(childIndex);
        if (pos == 0) {
            Page c = p.getChildPage(childIndex);
            if (c.getVersion() < minVersion) {
                return true;
            }
        } else if (map.getStore().getChunk(pos).version < minVersion) {
            return true;
        }
        return false;
    }

}
