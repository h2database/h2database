/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store.fuzz;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;

/**
 * All concrete {@link FuzzOperation} implementations as static inner classes.
 * Also contains the {@link #fromLine(String)} factory for deserialization.
 */
public final class FuzzOperations {

    private FuzzOperations() {}

    public static FuzzOperation fromLine(String line) {
        String[] parts = line.split(" ", 2);
        String opName = parts[0];
        String rest = parts.length > 1 ? parts[1] : "";
        switch (opName) {
        case "put":        return Put.parse(rest);
        case "remove":     return Remove.parse(rest);
        case "clear":      return new Clear();
        case "commit":     return new Commit();
        case "rollback":   return new Rollback();
        case "compact":    return Compact.parse(rest);
        case "compactFile":return new CompactFile();
        case "reopen":     return new Reopen();
        case "auxChurn":   return AuxChurn.parse(rest);
        case "openCursor": return OpenCursor.parse(rest);
        case "advanceCursor": return AdvanceCursor.parse(rest);
        default:
            throw new IllegalArgumentException("Unknown op: " + opName);
        }
    }

    public static final class Put implements FuzzOperation {
        public final int key;
        public final int length;

        public Put(int key, int length) {
            this.key = key;
            this.length = length;
        }

        @Override
        public void execute(FuzzRunContext ctx) {
            String value = "x".repeat(length);
            ctx.map.put(key, value);
            ctx.shadow.put(key, value);
            ctx.spotCheck(key);
        }

        @Override
        public String toLine() {
            return "put k=" + key + " len=" + length;
        }

        static Put parse(String rest) {
            int lenIdx = rest.indexOf(" len=");
            int key = Integer.parseInt(rest.substring(2, lenIdx));
            int length = Integer.parseInt(rest.substring(lenIdx + 5));
            return new Put(key, length);
        }
    }

    public static final class Remove implements FuzzOperation {
        public final int key;

        public Remove(int key) {
            this.key = key;
        }

        @Override
        public void execute(FuzzRunContext ctx) {
            ctx.map.remove(key);
            ctx.shadow.remove(key);
            ctx.spotCheck(key);
        }

        @Override
        public String toLine() {
            return "remove k=" + key;
        }

        static Remove parse(String rest) {
            return new Remove(Integer.parseInt(FuzzParseUtil.kv(rest, "k")));
        }
    }

    public static final class Clear implements FuzzOperation {
        @Override
        public void execute(FuzzRunContext ctx) {
            ctx.map.clear();
            ctx.shadow.clear();
            ctx.spotCheck(0);
        }

        @Override
        public String toLine() {
            return "clear";
        }
    }

    public static final class Commit implements FuzzOperation {
        @Override
        public void execute(FuzzRunContext ctx) {
            ctx.store.commit();
            ctx.committedShadow = new java.util.TreeMap<>(ctx.shadow);
        }

        @Override
        public String toLine() {
            return "commit";
        }
    }

    public static final class Rollback implements FuzzOperation {
        @Override
        public void execute(FuzzRunContext ctx) {
            ctx.store.rollback();
            ctx.shadow = new java.util.TreeMap<>(ctx.committedShadow);
            ctx.cursors.clear();
        }

        @Override
        public String toLine() {
            return "rollback";
        }
    }

    public static final class Compact implements FuzzOperation {
        public final int targetFillRate;
        public final int write;

        public Compact(int targetFillRate, int write) {
            this.targetFillRate = targetFillRate;
            this.write = write;
        }

        @Override
        public void execute(FuzzRunContext ctx) {
            ctx.store.compact(targetFillRate, write);
        }

        @Override
        public String toLine() {
            return "compact targetFillRate=" + targetFillRate + " write=" + write;
        }

        static Compact parse(String rest) {
            return new Compact(
                    Integer.parseInt(FuzzParseUtil.kv(rest, "targetFillRate")),
                    Integer.parseInt(FuzzParseUtil.kv(rest, "write")));
        }
    }

    public static final class CompactFile implements FuzzOperation {
        @Override
        public void execute(FuzzRunContext ctx) {
            ctx.store.commit();
            ctx.committedShadow = new java.util.TreeMap<>(ctx.shadow);
            ctx.store.compactFile(1000);
        }

        @Override
        public String toLine() {
            return "compactFile";
        }
    }

    public static final class Reopen implements FuzzOperation {
        @Override
        public void execute(FuzzRunContext ctx) {
            ctx.cursors.clear();
            ctx.store.commit();
            ctx.committedShadow = new java.util.TreeMap<>(ctx.shadow);
            ctx.store.close();
            ctx.store = ctx.config.openStore(ctx.fileName);
            ctx.map = ctx.store.openMap("data");
        }

        @Override
        public String toLine() {
            return "reopen";
        }
    }

    public static final class AuxChurn implements FuzzOperation {
        public final int writes;
        public final int deletes;
        public final int len;
        public final boolean remove;

        public AuxChurn(int writes, int deletes, int len, boolean remove) {
            this.writes = writes;
            this.deletes = deletes;
            this.len = len;
            this.remove = remove;
        }

        @Override
        public void execute(FuzzRunContext ctx) {
            MVMap<Integer, String> aux = ctx.store.openMap("aux" + ctx.auxMapCounter++);
            String value = "x".repeat(len);
            for (int i = 0; i < writes; i++) {
                aux.put(i, value);
            }
            for (int i = 0; i < deletes; i++) {
                aux.remove(i);
            }
            if (remove) {
                ctx.store.removeMap(aux);
            }
        }

        @Override
        public String toLine() {
            return "auxChurn writes=" + writes
                + " deletes=" + deletes
                + " len=" + len
                + " remove=" + remove;
        }

        static AuxChurn parse(String rest) {
            return new AuxChurn(
                Integer.parseInt(FuzzParseUtil.kv(rest, "writes")),
                Integer.parseInt(FuzzParseUtil.kv(rest, "deletes")),
                Integer.parseInt(FuzzParseUtil.kv(rest, "len")),
                Boolean.parseBoolean(FuzzParseUtil.kv(rest, "remove")));
        }
    }

    public static final class OpenCursor implements FuzzOperation {
        /** null means scan from beginning */
        public final Integer from;
        public final int opIndex;

        public OpenCursor(Integer from, int opIndex) {
            this.from = from;
            this.opIndex = opIndex;
        }

        @Override
        public void execute(FuzzRunContext ctx) {
            if (ctx.cursors.size() >= 8) {
                ctx.cursors.removeFirst();
            }
            Cursor<Integer, String> c = ctx.map.cursor(from);
            java.util.ArrayList<Map.Entry<Integer, String>> snapshot = new java.util.ArrayList<>(
                    (from == null ? ctx.shadow : ctx.shadow.tailMap(from)).entrySet());
            ctx.cursors.addLast(new FuzzRunContext.OpenCursor(c, snapshot.iterator(), ctx.opIndex));
        }

        @Override
        public String toLine() {
            return "openCursor from=" + (from == null ? "null" : from);
        }

        static OpenCursor parse(String rest) {
            String fromStr = FuzzParseUtil.kv(rest, "from");
            Integer from = "null".equals(fromStr) ? null : Integer.parseInt(fromStr);
            return new OpenCursor(from, -1);
        }
    }

    public static final class AdvanceCursor implements FuzzOperation {
        /** "first" or "last" */
        public final String which;
        public final int steps;

        public AdvanceCursor(String which, int steps) {
            this.which = which;
            this.steps = steps;
        }

        @Override
        public void execute(FuzzRunContext ctx) {
            if (ctx.cursors.isEmpty()) {
                return;
            }
            FuzzRunContext.OpenCursor oc = "first".equals(which)
                    ? ctx.cursors.peekFirst() : ctx.cursors.peekLast();
            for (int i = 0; i < steps; i++) {
                if (!ctx.advanceCursorStep(oc)) {
                    return;
                }
            }
        }

        @Override
        public String toLine() {
            return "advanceCursor which=" + which + " steps=" + steps;
        }

        static AdvanceCursor parse(String rest) {
            return new AdvanceCursor(FuzzParseUtil.kv(rest, "which"), Integer.parseInt(FuzzParseUtil.kv(rest, "steps")));
        }
    }
}
