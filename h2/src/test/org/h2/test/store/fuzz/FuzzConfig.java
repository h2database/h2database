/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store.fuzz;

import org.h2.mvstore.MVStore;

/**
 * Store configuration for a fuzz run. Serializes to/from a single line so it
 * can be saved at the top of a {@link FuzzScript} trace file.
 */
public final class FuzzConfig {

    public final boolean autoCommit;
    public final int keysPerPage;
    public final int autoCommitBufferKb;
    public final int versionsToKeep;
    public final int retentionTime;

    public FuzzConfig(boolean autoCommit, int keysPerPage, int autoCommitBufferKb,
            int versionsToKeep, int retentionTime) {
        this.autoCommit = autoCommit;
        this.keysPerPage = keysPerPage;
        this.autoCommitBufferKb = autoCommitBufferKb;
        this.versionsToKeep = versionsToKeep;
        this.retentionTime = retentionTime;
    }

    public MVStore openStore(String fileName) {
        MVStore.Builder builder = new MVStore.Builder()
                .fileName(fileName)
                .keysPerPage(keysPerPage)
                .cacheSize(1);
        if (autoCommit) {
            builder.autoCommitBufferSize(autoCommitBufferKb);
        } else {
            builder.autoCommitDisabled().autoCommitBufferSize(0);
        }
        MVStore s = builder.open();
        s.setVersionsToKeep(versionsToKeep);
        s.setRetentionTime(retentionTime);
        return s;
    }

    public String toLine() {
        return "config autoCommit=" + autoCommit
                + " keysPerPage=" + keysPerPage
                + " autoCommitBufferKb=" + autoCommitBufferKb
                + " versionsToKeep=" + versionsToKeep
                + " retentionTime=" + retentionTime;
    }

    public static FuzzConfig fromLine(String line) {
        // "config autoCommit=true keysPerPage=10 ..."
        String[] parts = line.split(" ");
        boolean autoCommit = Boolean.parseBoolean(value(parts, "autoCommit"));
        int keysPerPage = Integer.parseInt(value(parts, "keysPerPage"));
        int autoCommitBufferKb = Integer.parseInt(value(parts, "autoCommitBufferKb"));
        int versionsToKeep = Integer.parseInt(value(parts, "versionsToKeep"));
        int retentionTime = Integer.parseInt(value(parts, "retentionTime"));
        return new FuzzConfig(autoCommit, keysPerPage, autoCommitBufferKb,
                versionsToKeep, retentionTime);
    }

    private static String value(String[] parts, String key) {
        String prefix = key + "=";
        for (String p : parts) {
            if (p.startsWith(prefix)) {
                return p.substring(prefix.length());
            }
        }
        throw new IllegalArgumentException("Missing key: " + key);
    }

    @Override
    public String toString() {
        return toLine();
    }
}
