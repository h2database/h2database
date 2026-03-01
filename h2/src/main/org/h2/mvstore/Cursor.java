/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A cursor to iterate over elements in ascending or descending order.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class Cursor<K,V> implements Iterator<K> {

    /**
     * Minimum number of not-yet-visited sibling children that must exist at an
     * internal node before we bother submitting a prefetch.  Below this
     * threshold the range is too short for async I/O scheduling to pay off:
     * the ForkJoin submission overhead alone exceeds the latency hidden.
     * <p>
     * Value of 8 was chosen empirically — it suppresses prefetch at the tail
     * of nearly-exhausted subtrees while still firing on all nodes above the
     * lowest level of a typical wide B-tree.
     */
    private static final int PREFETCH_MIN_SIBLINGS = 8;

    /**
     * Maximum descent depth at which prefetch is triggered.  Depth 0 is the
     * node the cursor is currently sitting at; depth 1 is its children, etc.
     * <p>
     * Prefetch at depth 0 covers an entire subtree worth of future reads —
     * exactly what is useful.  Prefetch at deeper levels re-submits work
     * already queued by a shallower call, flooding the I/O queue and causing
     * the demand-read thread to race its own background tasks (the root cause
     * of the {@code coldCacheScan} regression).  Limiting to depth 1 gives
     * two levels of look-ahead: enough for wide trees, harmless for narrow ones.
     */
    private static final int MAX_PREFETCH_DEPTH = 1;

    private final boolean reverse;
    private final K to;
    private CursorPos<K,V> cursorPos;
    private CursorPos<K,V> keeper;
    private K current;
    private K last;
    private V lastValue;
    private Page<K,V> lastPage;


    public Cursor(RootReference<K,V> rootReference, K from, K to) {
        this(rootReference, from, to, false);
    }

    /**
     * @param rootReference of the tree
     * @param from starting key (inclusive), if null start from the first / last key
     * @param to ending key (inclusive), if null there is no boundary
     * @param reverse true if tree should be iterated in key's descending order
     */
    public Cursor(RootReference<K,V> rootReference, K from, K to, boolean reverse) {
        this.lastPage = rootReference.root;
        this.cursorPos = traverseDown(lastPage, from, reverse);
        this.to = to;
        this.reverse = reverse;
    }

    @Override
    public boolean hasNext() {
        if (cursorPos != null) {
            int increment = reverse ? -1 : 1;
            while (current == null) {
                Page<K,V> page = cursorPos.page;
                int index = cursorPos.index;
                if (reverse ? index < 0 : index >= upperBound(page)) {
                    // traversal of this page is over, going up a level or stop if at the root already
                    CursorPos<K,V> tmp = cursorPos;
                    cursorPos = cursorPos.parent;
                    if (cursorPos == null) {
                        return false;
                    }
                    tmp.parent = keeper;
                    keeper = tmp;
                } else {
                    // traverse down to the leaf taking the leftmost path
                    int descentDepth = 0;
                    while (!page.isLeaf()) {
                        if (descentDepth < MAX_PREFETCH_DEPTH) {
                            maybePrefetchSiblings(page, index);
                        }
                        page = page.getChildPage(index);
                        index = reverse ? upperBound(page) - 1 : 0;
                        if (keeper == null) {
                            cursorPos = new CursorPos<>(page, index, cursorPos);
                        } else {
                            CursorPos<K,V> tmp = keeper;
                            keeper = keeper.parent;
                            tmp.parent = cursorPos;
                            tmp.page = page;
                            tmp.index = index;
                            cursorPos = tmp;
                        }
                        ++descentDepth;
                    }
                    if (reverse ? index >= 0 : index < page.getKeyCount()) {
                        K key = page.getKey(index);
                        if (to != null && Integer.signum(page.map.getKeyType().compare(key, to)) == increment) {
                            return false;
                        }
                        current = last = key;
                        lastValue = page.getValue(index);
                        lastPage = page;
                    }
                }
                cursorPos.index += increment;
            }
        }
        return current != null;
    }

    @Override
    public K next() {
        if(!hasNext()) {
            throw new NoSuchElementException();
        }
        current = null;
        return last;
    }

    /**
     * Get the last read key if there was one.
     *
     * @return the key or null
     */
    public K getKey() {
        return last;
    }

    /**
     * Get the last read value if there was one.
     *
     * @return the value or null
     */
    public V getValue() {
        return lastValue;
    }

    /**
     * Get the page where last retrieved key is located.
     *
     * @return the page
     */
    @SuppressWarnings("unused")
    Page<K,V> getPage() {
        return lastPage;
    }

    /**
     * Skip over that many entries. This method is relatively fast (for this map
     * implementation) even if many entries need to be skipped.
     *
     * @param n the number of entries to skip
     */
    public void skip(long n) {
        if (n < 10) {
            while (n-- > 0 && hasNext()) {
                next();
            }
        } else if(hasNext()) {
            assert cursorPos != null;
            CursorPos<K,V> cp = cursorPos;
            CursorPos<K,V> parent;
            while ((parent = cp.parent) != null) cp = parent;
            Page<K,V> root = cp.page;
            MVMap<K,V> map = root.map;
            long index = map.getKeyIndex(next());
            last = map.getKey(index + (reverse ? -n : n));
            this.cursorPos = traverseDown(root, last, reverse);
        }
    }

    /**
     * Submits a best-effort background prefetch for the sibling children of
     * {@code page} that the cursor will visit <em>after</em> it finishes with
     * the subtree rooted at child {@code currentIndex}.
     *
     * <h3>Upper-bound analysis</h3>
     * Instead of a fixed-size sliding window, the method uses the B-tree's own
     * separator keys to determine which siblings are actually within the
     * cursor's {@link #to} bound.  For a forward cursor, child {@code i}
     * (i &gt; 0) has a minimum key of {@code page.getKey(i-1)}; as soon as that
     * separator exceeds {@code to} we stop collecting siblings.  Reverse
     * iteration applies the symmetric check on the maximum key.  This avoids
     * issuing any I/O for siblings that lie entirely outside the requested
     * range.
     *
     * <h3>In-memory / warm-cache short-circuit</h3>
     * <ul>
     *   <li>If the store has no backing file, return immediately — all pages
     *       are already in RAM.</li>
     *   <li>If the first candidate sibling is already in the page cache,
     *       assume the working set is warm and skip prefetch entirely,
     *       eliminating the ForkJoin task-submission overhead for hot
     *       iterators.</li>
     *   <li>Unsaved child positions (pos&nbsp;==&nbsp;0) are skipped
     *       individually — they are already in the write buffer.</li>
     * </ul>
     *
     * @param page         the internal node being descended
     * @param currentIndex index of the child we are about to follow
     */
    private void maybePrefetchSiblings(Page<K,V> page, int currentIndex) {
        // Prefetch only makes sense for bounded range scans.  Unbounded
        // full-table scans (to == null) already saturate I/O on their own;
        // adding background reads doubles I/O traffic and hurts throughput.
        if (to == null) return;

        MVMap<K,V> map = page.map;

        // In-memory store: all pages are already in RAM, nothing to prefetch.
        if (!map.store.isPersistent()) return;

        FileStore<?> fs = map.store.getFileStore();
        if (fs == null) return;

        int childCount = map.getChildPageCount(page);

        // Identify the sibling range that will be visited after currentIndex.
        int sibStart, sibEnd;
        if (reverse) {
            sibStart = 0;
            sibEnd   = currentIndex;          // indices 0 .. currentIndex-1
        } else {
            sibStart = currentIndex + 1;
            sibEnd   = childCount;            // indices currentIndex+1 .. childCount-1
        }
        if (sibEnd - sibStart < PREFETCH_MIN_SIBLINGS) return;

        // ── Warm-cache short-circuit ────────────────────────────────────────
        // If the first candidate sibling is already in the page cache the
        // working set is hot; skip ForkJoin overhead entirely.
        long firstPos = page.getChildPagePos(sibStart);
        if (DataUtils.isPageSaved(firstPos) && fs.isPageCached(firstPos)) return;

        // ── Upper-bound analysis using separator keys ───────────────────────
        // NonLeaf page layout: keys[0..childCount-2] separate children.
        //   child[i] minimum key ≈ keys[i-1]   (for i > 0)
        //   child[i] maximum key ≈ keys[i]      (for i < childCount-1)
        //
        // Forward: stop before sibling i when keys[i-1] > to  (min of child > to)
        // Reverse: stop before sibling i when keys[i]   < to  (max of child < to)
        long[] positions = new long[sibEnd - sibStart];
        int idx = 0;
        for (int i = sibStart; i < sibEnd; i++) {
            if (to != null) {
                if (reverse) {
                    // child[i]'s maximum key ≈ keys[i] (exists when i < childCount-1)
                    if (i < childCount - 1) {
                        K maxKey = page.getKey(i);
                        if (page.map.getKeyType().compare(maxKey, to) < 0) break; // max < to → out of range
                    }
                } else {
                    // child[i]'s minimum key ≈ keys[i-1] (exists when i > 0)
                    if (i > 0) {
                        K minKey = page.getKey(i - 1);
                        if (page.map.getKeyType().compare(minKey, to) > 0) break; // min > to → out of range
                    }
                }
            }
            long pos = page.getChildPagePos(i);
            if (DataUtils.isPageSaved(pos)) {
                positions[idx++] = pos;
            }
        }
        if (idx == 0) return;
        if (idx < positions.length) {
            positions = Arrays.copyOf(positions, idx);
        }

        fs.prefetchPages(map, positions);
    }

    /**
     * Fetch the next entry that is equal or larger than the given key, starting
     * from the given page. This method returns the path.
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @param page to start from as a root
     * @param key to search for, null means search for the first available key
     * @param reverse true if traversal is in reverse direction, false otherwise
     * @return CursorPos representing path from the entry found,
     *         or from insertion point if not,
     *         all the way up to the root page provided
     */
    static <K,V> CursorPos<K,V> traverseDown(Page<K,V> page, K key, boolean reverse) {
        CursorPos<K,V> cursorPos = key != null ? CursorPos.traverseDown(page, key, null) :
                                   reverse ? page.getAppendCursorPos(null) : page.getPrependCursorPos(null);
        int index = cursorPos.index;
        if (index < 0) {
            index = ~index;
            if (reverse) {
                --index;
            }
            cursorPos.index = index;
        }
        return cursorPos;
    }

    private static <K,V> int upperBound(Page<K,V> page) {
        return page.isLeaf() ? page.getKeyCount() : page.map.getChildPageCount(page);
    }
}