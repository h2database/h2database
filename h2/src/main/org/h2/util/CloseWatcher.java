/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 * Iso8601:
 * Initial Developer: Robert Rathsack (firstName dot lastName at gmx dot de)
 */
package org.h2.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A phantom reference to watch for unclosed objects.
 */
public class CloseWatcher extends PhantomReference<Object> {

    /**
     * The queue (might be set to null at any time).
     */
    private static final ReferenceQueue<Object> queue = new ReferenceQueue<>();

    /**
     * The reference set. Must keep it, otherwise the references are garbage
     * collected first and thus never enqueued.
     */
    private static final Set<CloseWatcher> refs = Collections.synchronizedSet(new HashSet<>());

    /**
     * The stack trace of when the object was created. It is converted to a
     * string early on to avoid classloader problems (a classloader can't be
     * garbage collected if there is a static reference to one of its classes).
     */
    private String openStackTrace;

    /**
     * The closeable object.
     */
    private AutoCloseable closeable;

    public CloseWatcher(Object referent, ReferenceQueue<Object> q,
            AutoCloseable closeable) {
        super(referent, q);
        this.closeable = closeable;
    }

    /**
     * Check for an collected object.
     *
     * @return the first watcher
     */
    public static CloseWatcher pollUnclosed() {
        while (true) {
            CloseWatcher cw = (CloseWatcher) queue.poll();
            if (cw == null) {
                return null;
            }
            if (refs != null) {
                refs.remove(cw);
            }
            if (cw.closeable != null) {
                return cw;
            }
        }
    }

    /**
     * Register an object. Before calling this method, pollUnclosed() should be
     * called in a loop to remove old references.
     *
     * @param o the object
     * @param closeable the object to close
     * @param stackTrace whether the stack trace should be registered (this is
     *            relatively slow)
     * @return the close watcher
     */
    public static CloseWatcher register(Object o, AutoCloseable closeable, boolean stackTrace) {
        CloseWatcher cw = new CloseWatcher(o, queue, closeable);
        if (stackTrace) {
            Exception e = new Exception("Open Stack Trace");
            StringWriter s = new StringWriter();
            e.printStackTrace(new PrintWriter(s));
            cw.openStackTrace = s.toString();
        }
        refs.add(cw);
        return cw;
    }

    /**
     * Unregister an object, so it is no longer tracked.
     *
     * @param w the reference
     */
    public static void unregister(CloseWatcher w) {
        w.closeable = null;
        refs.remove(w);
    }

    /**
     * Get the open stack trace or null if none.
     *
     * @return the open stack trace
     */
    public String getOpenStackTrace() {
        return openStackTrace;
    }

    public AutoCloseable getCloseable() {
        return closeable;
    }

}
