/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

/**
 * A file system that re-opens and re-tries the operation if the file was
 * closed, because a thread was interrupted.
 * <p>
 * This will clear the interrupt flag. It is mainly useful for applications that
 * call {@link java.lang.Thread#interrupt()} by mistake.
 * </p>
 */
package org.h2.store.fs.retry;
