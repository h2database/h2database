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
import org.h2.mvstore.MVStore;
import org.h2.store.fs.FileUtils;
import org.h2.test.store.fuzz.FuzzRunContext.OpenCursor;

/**
 * All concrete {@link FuzzOperation} implementations as static inner classes.
 * Also contains the {@link #fromLine(String)} factory for deserialization.
 */
public final class FuzzOperations {

    private FuzzOperations() {}

    // -------------------------------------------------------------------------
    // Deserialization factory
    // -------------------------------------------------------------------------

    public static FuzzOperation fromLine(String line) {
        String[] parts = line.split(" ", 2);
        String opName = parts[0];
        String rest = parts.length > 1 ? parts[1] : "";
        switch (opName) {
        case "put":        return Put.parse(rest);
        case "putBig":     return PutBig.parse(rest);
        case "remove":     return Remove.parse(rest);
        case "rangePut":   return RangePut.parse(rest);
        case "rangeRemove":return RangeRemove.parse(rest);
        case "clear":      return new Clear();
        case "commit":     return new Commit();
        case "rollback":   return new Rollback();
        case "compact":    return Compact.parse(rest);
        case "compactFile":return new CompactFile();
        case "reopen":     return new Reopen();
        case "openCursor": return OpenCursorOp.parse(rest);
        case "advanceCursor": return AdvanceCursor.parse(rest);
        case "auxChurn":   return AuxChurn.parse(rest);
        default:
            throw new IllegalArgumentException("Unknown op: " + opName);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static String kv(String rest, String key) {
        for (String part : rest.split(" ")) {
            if (part.startsWith(key + "=")) {
                return part.substring(key.length() + 1);
            }
        }
        throw new IllegalArgumentException("Missing key '" + key + "' in: " + rest);
    }

    private static String kvOpt(String rest, String key, String defaultValue) {
        for (String part : rest.split(" ")) {
            if (part.startsWith(key + "=")) {
                return part.substring(key.length() + 1);
            }
        }
        return defaultValue;
    }

    static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }

    static byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

    // -------------------------------------------------------------------------
    // put
    // -------------------------------------------------------------------------

    public static final class Put implements FuzzOperation {
        public final int key;
        public final String value;

        public Put(int key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public void execute(FuzzRunContext ctx) {
            ctx.map.put(key, value);
            ctx.shadow.put(key, value);
            ctx.spotCheck(key);
        }

        @Override
        public String toLine() {
            return "put k=" + key + " v=" + value;
        }

        static Put parse(String rest) {
            // "k=42 v=42_0_vvvv" — value may contain spaces only if quoted,
            // but our values never contain spaces, so split on first " v=" is safe
            int vIdx = rest.indexOf(" v=");
            int key = Integer.parseInt(rest.substring(2, vIdx));
            String value = rest.substring(vIdx + 3);
            return new Put(key, value);
        }
    }

    // -------------------------------------------------------------------------
    // putBig
    // -------------------------------------------------------------------------

    public static final class PutBig implements FuzzOperation {
        public final int key;
        public final String value;

        public PutBig(int key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public void execute(FuzzRunContext ctx) {
            ctx.map.put(key, value);
            ctx.shadow.put(key, value);
            ctx.spotCheck(key);
        }

        @Override
        public String toLine() {
            return "putBig k=" + key + " v=" + value;
        }

        static PutBig parse(String rest) {
            int vIdx = rest.indexOf(" v=");
            int key = Integer.parseInt(rest.substring(2, vIdx));
            String value = rest.substring(vIdx + 3);
            return new PutBig(key, value);
        }
    }

    // -------------------------------------------------------------------------
    // remove
    // -------------------------------------------------------------------------

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
            return new Remove(Integer.parseInt(kv(rest, "k")));
        }
    }

    // -------------------------------------------------------------------------
    // rangePut
    // -------------------------------------------------------------------------

    public static final class RangePut implements FuzzOperation {
        /** step is +1 or -1 */
        public final int step;
        /** Keys and values in iteration order (key already concrete). */
        public final Map<Integer, String> entries;

        public RangePut(int step, Map<Integer, String> entries) {
            this.step = step;
            this.entries = entries;
        }

        @Override
        public void execute(FuzzRunContext ctx) {
            int lastKey = -1;
            for (Map.Entry<Integer, String> e : entries.entrySet()) {
                ctx.map.put(e.getKey(), e.getValue());
                ctx.shadow.put(e.getKey(), e.getValue());
                lastKey = e.getKey();
            }
            if (lastKey >= 0) {
                ctx.spotCheck(lastKey);
            }
        }

        @Override
        public String toLine() {
            StringBuilder sb = new StringBuilder("rangePut step=").append(step).append(" entries=");
            boolean first = true;
            for (Map.Entry<Integer, String> e : entries.entrySet()) {
                if (!first) sb.append(',');
                sb.append(e.getKey()).append(':').append(e.getValue());
                first = false;
            }
            return sb.toString();
        }

        static RangePut parse(String rest) {
            int step = Integer.parseInt(kv(rest, "step"));
            String entriesPart = kv(rest, "entries");
            Map<Integer, String> entries = new LinkedHashMap<>();
            if (!entriesPart.isEmpty()) {
                for (String pair : entriesPart.split(",")) {
                    int colon = pair.indexOf(':');
                    int k = Integer.parseInt(pair.substring(0, colon));
                    String v = pair.substring(colon + 1);
                    entries.put(k, v);
                }
            }
            return new RangePut(step, entries);
        }
    }

    // -------------------------------------------------------------------------
    // rangeRemove
    // -------------------------------------------------------------------------

    public static final class RangeRemove implements FuzzOperation {
        public final int step;
        public final List<Integer> keys;

        public RangeRemove(int step, List<Integer> keys) {
            this.step = step;
            this.keys = keys;
        }

        @Override
        public void execute(FuzzRunContext ctx) {
            int lastKey = -1;
            for (int k : keys) {
                ctx.map.remove(k);
                ctx.shadow.remove(k);
                lastKey = k;
            }
            if (lastKey >= 0) {
                ctx.spotCheck(lastKey);
            }
        }

        @Override
        public String toLine() {
            StringBuilder sb = new StringBuilder("rangeRemove step=").append(step).append(" keys=");
            for (int i = 0; i < keys.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append(keys.get(i));
            }
            return sb.toString();
        }

        static RangeRemove parse(String rest) {
            int step = Integer.parseInt(kv(rest, "step"));
            String keysPart = kv(rest, "keys");
            List<Integer> keys = new ArrayList<>();
            if (!keysPart.isEmpty()) {
                for (String k : keysPart.split(",")) {
                    keys.add(Integer.parseInt(k));
                }
            }
            return new RangeRemove(step, keys);
        }
    }

    // -------------------------------------------------------------------------
    // clear
    // -------------------------------------------------------------------------

    public static final class Clear implements FuzzOperation {
        @Override
        public void execute(FuzzRunContext ctx) {
            // pick any key for spot check; 0 will be absent after clear
            ctx.map.clear();
            ctx.shadow.clear();
            ctx.spotCheck(0);
        }

        @Override
        public String toLine() {
            return "clear";
        }
    }

    // -------------------------------------------------------------------------
    // commit
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // rollback
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // compact
    // -------------------------------------------------------------------------

    public static final class Compact implements FuzzOperation {
        public final int fillPercent;
        public final int maxMoveSize;

        public Compact(int fillPercent, int maxMoveSize) {
            this.fillPercent = fillPercent;
            this.maxMoveSize = maxMoveSize;
        }

        @Override
        public void execute(FuzzRunContext ctx) {
            ctx.store.compact(fillPercent, maxMoveSize);
        }

        @Override
        public String toLine() {
            return "compact fillPercent=" + fillPercent + " maxMoveSize=" + maxMoveSize;
        }

        static Compact parse(String rest) {
            return new Compact(
                    Integer.parseInt(kv(rest, "fillPercent")),
                    Integer.parseInt(kv(rest, "maxMoveSize")));
        }
    }

    // -------------------------------------------------------------------------
    // compactFile
    // -------------------------------------------------------------------------

    public static final class CompactFile implements FuzzOperation {
        @Override
        public void execute(FuzzRunContext ctx) {
            ctx.store.commit();
            ctx.committedShadow = new java.util.TreeMap<>(ctx.shadow);
            ctx.store.compactFile(200);
        }

        @Override
        public String toLine() {
            return "compactFile";
        }
    }

    // -------------------------------------------------------------------------
    // reopen
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // openCursor
    // -------------------------------------------------------------------------

    public static final class OpenCursorOp implements FuzzOperation {
        /** null means scan from beginning */
        public final Integer from;
        public final int opIndex;

        public OpenCursorOp(Integer from, int opIndex) {
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
            ctx.cursors.addLast(new OpenCursor(c, snapshot.iterator(), ctx.opIndex));
        }

        @Override
        public String toLine() {
            return "openCursor from=" + (from == null ? "null" : from);
        }

        static OpenCursorOp parse(String rest) {
            String fromStr = kv(rest, "from");
            Integer from = "null".equals(fromStr) ? null : Integer.parseInt(fromStr);
            // opIndex is not stored in file; it's set from ctx.opIndex during replay
            return new OpenCursorOp(from, -1);
        }
    }

    // -------------------------------------------------------------------------
    // advanceCursor
    // -------------------------------------------------------------------------

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
            OpenCursor oc = "first".equals(which)
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
            return new AdvanceCursor(kv(rest, "which"), Integer.parseInt(kv(rest, "steps")));
        }
    }

    // -------------------------------------------------------------------------
    // auxChurn
    // -------------------------------------------------------------------------

    public static final class AuxChurn implements FuzzOperation {
        public static final class Entry {
            public final boolean isPut;
            public final int key;
            public final byte[] value; // null for remove

            Entry(int key, byte[] value) {
                this.isPut = true;
                this.key = key;
                this.value = value;
            }

            Entry(int key) {
                this.isPut = false;
                this.key = key;
                this.value = null;
            }
        }

        public final List<Entry> entries;
        public final boolean removeMap;

        public AuxChurn(List<Entry> entries, boolean removeMap) {
            this.entries = entries;
            this.removeMap = removeMap;
        }

        @Override
        public void execute(FuzzRunContext ctx) {
            MVMap<Integer, byte[]> aux = ctx.store.openMap("aux");
            for (Entry e : entries) {
                if (e.isPut) {
                    aux.put(e.key, e.value);
                } else {
                    aux.remove(e.key);
                }
            }
            if (removeMap) {
                ctx.store.removeMap(aux);
            }
        }

        @Override
        public String toLine() {
            StringBuilder sb = new StringBuilder("auxChurn removeMap=").append(removeMap)
                    .append(" entries=");
            boolean first = true;
            for (Entry e : entries) {
                if (!first) sb.append(',');
                if (e.isPut) {
                    sb.append("put:").append(e.key).append(':').append(bytesToHex(e.value));
                } else {
                    sb.append("remove:").append(e.key);
                }
                first = false;
            }
            return sb.toString();
        }

        static AuxChurn parse(String rest) {
            boolean removeMap = Boolean.parseBoolean(kv(rest, "removeMap"));
            String entriesPart = kvOpt(rest, "entries", "");
            List<Entry> entries = new ArrayList<>();
            if (!entriesPart.isEmpty()) {
                for (String token : entriesPart.split(",")) {
                    String[] parts = token.split(":", 3);
                    if ("put".equals(parts[0])) {
                        entries.add(new Entry(Integer.parseInt(parts[1]),
                                hexToBytes(parts[2])));
                    } else {
                        entries.add(new Entry(Integer.parseInt(parts[1])));
                    }
                }
            }
            return new AuxChurn(entries, removeMap);
        }
    }
}
