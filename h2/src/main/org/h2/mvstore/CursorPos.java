/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

/**
 * A position in a cursor.
 * Instance represents a node in the linked list, which traces path
 * from a specific (target) key within a leaf node all the way up to te root
 * (bottom up path).
 */
public final class CursorPos<K,V> {

    /**
     * The page at the current level.
     */
    public Page<K,V> page;

    /**
     * Index of the key (within page above) used to go down to a lower level
     * in case of intermediate nodes, or index of the target key for leaf a node.
     * In a later case, it could be negative, if the key is not present.
     */
    public int index;

    /**
     * Next node in the linked list, representing the position within parent level,
     * or null, if we are at the root level already.
     */
    public CursorPos<K,V> parent;


    public CursorPos(Page<K,V> page, int index, CursorPos<K,V> parent) {
        this.page = page;
        this.index = index;
        this.parent = parent;
    }

    /**
     * Searches for a given key and creates a breadcrumb trail through a B-tree
     * rooted at a given Page. Resulting path starts at "insertion point" for a
     * given key and goes back to the root.
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @param page      root of the tree
     * @param key       the key to search for
     * @return head of the CursorPos chain (insertion point)
     */
    static <K,V> CursorPos<K,V> traverseDown(Page<K,V> page, K key, CursorPos<K,V> existing) {
        if (existing != null) {
            assert existing.page.isLeaf();
            existing = existing.reverse(null);
        }
        CursorPos<K,V> cursorPos = null;
        for(;;) {
            int index;
            if (existing == null) {
                index = page.calculateTraversalIndex(key);
                cursorPos = new CursorPos<>(page, index, cursorPos);
            } else {
                Page<K, V> existingPage = existing.page;
                if (existingPage == page) {
                    // If we hit exactly the same page, as previous time, that means that subtree under this page
                    // also hasn't been modified since last attempt. Further traversal therefore is going to follow
                    // exactly same path, so lets just copy it from existing CursorPos chain
                    cursorPos = existing.reverse(cursorPos);
                    assert cursorPos.page.isLeaf();
                    return cursorPos;
                }
                CursorPos<K, V> temp = existing.parent;
                existing.parent = cursorPos;
                existing.page = page;
                cursorPos = existing;
                existing = temp;
                // if we hit page with exact set of keys, as last time,
                // there is no need to do a key search again, use previous result
                if (!page.sameKeys(existingPage)) {
                    cursorPos.index = page.calculateTraversalIndex(key);
                }
                index = cursorPos.index;
            }
            if (page.isLeaf()) {
                assert cursorPos.page.isLeaf();
                return cursorPos;
            }
            page = page.getChildPage(index);
        }
    }

    /**
     * Calculate the memory used by changes that are not yet stored.
     *
     * @param version the version
     * @return the amount of memory
     */
    int processRemovalInfo(long version) {
        int unsavedMemory = 0;
        for (CursorPos<K,V> head = this; head != null; head = head.parent) {
            unsavedMemory += head.page.removePage(version);
        }
        return unsavedMemory;
    }

    private CursorPos<K,V> reverse(CursorPos<K,V> alreadyReversed) {
        CursorPos<K, V> reversed = parent == null ? this : parent.reverse(this);
        parent = alreadyReversed;
        return reversed;
    }

    @Override
    public String toString() {
        return "CursorPos{" +
                "page=" + page +
                ", index=" + index +
                ", parent=" + parent +
                '}';
    }
}

