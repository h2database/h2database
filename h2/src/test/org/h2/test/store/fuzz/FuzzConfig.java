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
        boolean autoCommit = Boolean.parseBoolean(FuzzParseUtil.kv(line, "autoCommit"));
        int keysPerPage = Integer.parseInt(FuzzParseUtil.kv(line, "keysPerPage"));
        int autoCommitBufferKb = Integer.parseInt(FuzzParseUtil.kv(line, "autoCommitBufferKb"));
        int versionsToKeep = Integer.parseInt(FuzzParseUtil.kv(line, "versionsToKeep"));
        int retentionTime = Integer.parseInt(FuzzParseUtil.kv(line, "retentionTime"));
        return new FuzzConfig(autoCommit, keysPerPage, autoCommitBufferKb,
                versionsToKeep, retentionTime);
    }

    @Override
    public String toString() {
        return toLine();
    }
}
