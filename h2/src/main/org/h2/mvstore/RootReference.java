/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

/**
 * Class RootReference is an immutable structure to represent state of the MVMap as a whole
 * (not related to a particular B-Tree node).
 * Single structure would allow for non-blocking atomic state change.
 * The most important part of it is a reference to the root node.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public final class RootReference
{
    /**
     * The root page.
     */
    public final Page root;
    /**
     * The version used for writing.
     */
    public final long version;
    /**
     * Counter of reenterant locks.
     */
    private final byte holdCount;
    /**
     * Lock owner thread id.
     */
    private final long ownerId;
    /**
     * Reference to the previous root in the chain.
     * That is the last root of the previous version, which had any data changes.
     * Versions without any data changes are dropped from the chain, as it built.
     */
    volatile RootReference previous;
    /**
     * Counter for successful root updates.
     */
    final long updateCounter;
    /**
     * Counter for attempted root updates.
     */
    final long updateAttemptCounter;
    /**
     * Size of the occupied part of the append buffer.
     */
    private final byte appendCounter;


    // This one is used to set root initially and for r/o snapshots
    RootReference(Page root, long version) {
        this.root = root;
        this.version = version;
        this.previous = null;
        this.updateCounter = 1;
        this.updateAttemptCounter = 1;
        this.holdCount = 0;
        this.ownerId = 0;
        this.appendCounter = 0;
    }

    private RootReference(RootReference r, Page root, long updateAttemptCounter) {
        this.root = root;
        this.version = r.version;
        this.previous = r.previous;
        this.updateCounter = r.updateCounter + 1;
        this.updateAttemptCounter = r.updateAttemptCounter + updateAttemptCounter;
        this.holdCount = 0;
        this.ownerId = 0;
        this.appendCounter = r.appendCounter;
    }

    // This one is used for locking
    private RootReference(RootReference r, int attempt) {
        this.root = r.root;
        this.version = r.version;
        this.previous = r.previous;
        this.updateCounter = r.updateCounter + 1;
        this.updateAttemptCounter = r.updateAttemptCounter + attempt;
        assert r.holdCount == 0 || r.ownerId == Thread.currentThread().getId() //
                : Thread.currentThread().getId() + " " + r;
        this.holdCount = (byte)(r.holdCount + 1);
        this.ownerId = Thread.currentThread().getId();
        this.appendCounter = r.appendCounter;
    }

    // This one is used for unlocking
    private RootReference(RootReference r, Page root, boolean keepLocked, int appendCounter) {
        this.root = root;
        this.version = r.version;
        this.previous = r.previous;
        this.updateCounter = r.updateCounter;
        this.updateAttemptCounter = r.updateAttemptCounter;
        assert r.holdCount > 0 && r.ownerId == Thread.currentThread().getId() //
                : Thread.currentThread().getId() + " " + r;
        this.holdCount = (byte)(r.holdCount - (keepLocked ? 0 : 1));
        this.ownerId = this.holdCount == 0 ? 0 : Thread.currentThread().getId();
        this.appendCounter = (byte) appendCounter;
    }

    // This one is used for version change
    private RootReference(RootReference r, long version, int attempt) {
        RootReference previous = r;
        RootReference tmp;
        while ((tmp = previous.previous) != null && tmp.root == r.root) {
            previous = tmp;
        }
        this.root = r.root;
        this.version = version;
        this.previous = previous;
        this.updateCounter = r.updateCounter + 1;
        this.updateAttemptCounter = r.updateAttemptCounter + attempt;
        this.holdCount = r.holdCount == 0 ? 0 : (byte)(r.holdCount - 1);
        this.ownerId = this.holdCount == 0 ? 0 : r.ownerId;
        assert r.appendCounter == 0;
        this.appendCounter = 0;
    }

    /**
     * Try to unlock.
     *
     * @param newRootPage the new root page
     * @param attemptCounter the number of attempts so far
     * @return the new, unlocked, root reference, or null if not successful
     */
    RootReference updateRootPage(Page newRootPage, long attemptCounter) {
        if (holdCount == 0) {
            RootReference updatedRootReference = new RootReference(this, newRootPage, attemptCounter);
            if (root.map.compareAndSetRoot(this, updatedRootReference)) {
                return updatedRootReference;
            }
        }
        return null;
    }

    /**
     * Try to lock.
     *
     * @param attemptCounter the number of attempts so far
     * @return the new, locked, root reference, or null if not successful
     */
    RootReference tryLock(int attemptCounter) {
        if (holdCount == 0 || ownerId == Thread.currentThread().getId()) {
            RootReference lockedRootReference = new RootReference(this, attemptCounter);
            if (root.map.compareAndSetRoot(this, lockedRootReference)) {
                return lockedRootReference;
            }
        }
        return null;
    }

    /**
     * Try to unlock, and if successful update the version
     *
     * @param version the version
     * @param attempt the number of attempts so far
     * @return the new, unlocked and updated, root reference, or null if not successful
     */
    RootReference tryUnlockAndUpdateVersion(long version, int attempt) {
        if (holdCount == 0 || ownerId == Thread.currentThread().getId()) {
            RootReference updatedRootReference = new RootReference(this, version, attempt);
            if (root.map.compareAndSetRoot(this, updatedRootReference)) {
                return updatedRootReference;
            }
        }
        return null;
    }

    /**
     * Update the page, possibly keeping it locked.
     *
     * @param page the page
     * @param keepLocked whether to keep it locked
     * @param attempt the number of attempts so far
     * @return the new root reference, or null if not successful
     */
    RootReference updatePageAndLockedStatus(Page page, boolean keepLocked, int appendCounter) {
        assert isLockedByCurrentThread() : this;
        RootReference updatedRootReference = new RootReference(this, page, keepLocked, appendCounter);
        if (root.map.compareAndSetRoot(this, updatedRootReference)) {
            return updatedRootReference;
        }
        return null;
    }

    /**
     * Removed old versions that are not longer used.
     *
     * @param oldestVersionToKeep the oldest version that needs to be retained
     */
    void removeUnusedOldVersions(long oldestVersionToKeep) {
        // We need to keep at least one previous version (if any) here,
        // because in order to retain whole history of some version
        // we really need last root of the previous version.
        // Root labeled with version "X" is the LAST known root for that version
        // and therefore the FIRST known root for the version "X+1"
        for(RootReference rootRef = this; rootRef != null; rootRef = rootRef.previous) {
            if (rootRef.version < oldestVersionToKeep) {
                RootReference previous;
                assert (previous = rootRef.previous) == null || previous.getAppendCounter() == 0 //
                        : oldestVersionToKeep + " " + rootRef.previous;
                rootRef.previous = null;
            }
        }
    }

    boolean isLocked() {
        return holdCount != 0;
    }

    public boolean isLockedByCurrentThread() {
        return holdCount != 0 && ownerId == Thread.currentThread().getId();
    }

    long getVersion() {
        RootReference prev = previous;
        return prev == null || prev.root != root ||
                prev.appendCounter != appendCounter ?
                    version : prev.version;
    }

    /**
     * Does the root have changes since the specified version?
     *
     * @param version to check against
     * @return true if this root has unsaved changes
     */
    boolean hasChangesSince(long version) {
        return (root.isSaved() ? getAppendCounter() > 0 : getTotalCount() > 0) || getVersion() > version;
    }

    int getAppendCounter() {
        return appendCounter & 0xff;
    }

    /**
     * Whether flushing is needed.
     *
     * @return true if yes
     */
    public boolean needFlush() {
        return appendCounter != 0;
    }

    public long getTotalCount() {
        return root.getTotalCount() + getAppendCounter();
    }

    @Override
    public String toString() {
        return "RootReference(" + System.identityHashCode(root) +
                ", v=" + version +
                ", owner=" + ownerId + (ownerId == Thread.currentThread().getId() ? "(current)" : "") +
                ", holdCnt=" + holdCount +
                ", keys=" + root.getTotalCount() +
                ", append=" + getAppendCounter() +
                ")";
    }
}
