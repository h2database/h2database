/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import org.h2.constant.SysProperties;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;

/**
 * The cursor implementation for the multi-version index.
 */
public class MultiVersionCursor implements Cursor {

    private final MultiVersionIndex index;
    private final Session session;
    private final Cursor baseCursor, deltaCursor;
    private final Object sync;
    private SearchRow baseRow;
    private Row deltaRow;
    private boolean onBase;
    private boolean end;
    private boolean needNewDelta, needNewBase;

    MultiVersionCursor(Session session, MultiVersionIndex index, Cursor base, Cursor delta, Object sync) throws SQLException {
        this.session = session;
        this.index = index;
        this.baseCursor = base;
        this.deltaCursor = delta;
        this.sync = sync;
        needNewDelta = needNewBase = true;
    }

    private void loadNext(boolean base) throws SQLException {
        synchronized (sync) {
            if (base) {
                if (baseCursor.next()) {
                    baseRow = baseCursor.getSearchRow();
                } else {
                    baseRow = null;
                }
            } else {
                if (deltaCursor.next()) {
                    deltaRow = deltaCursor.get();
                } else {
                    deltaRow = null;
                }
            }
        }
    }

    public Row get() throws SQLException {
        synchronized (sync) {
            if (SysProperties.CHECK && end) {
                throw Message.getInternalError();
            }
            return onBase ? baseCursor.get() : deltaCursor.get();
        }
    }

    public int getPos() {
        synchronized (sync) {
            if (SysProperties.CHECK && end) {
                throw Message.getInternalError();
            }
            return onBase ? baseCursor.getPos() : deltaCursor.getPos();
        }
    }

    public SearchRow getSearchRow() throws SQLException {
        synchronized (sync) {
            if (SysProperties.CHECK && end) {
                throw Message.getInternalError();
            }
            return onBase ? baseCursor.getSearchRow() : deltaCursor.getSearchRow();
        }
    }

    public boolean next() throws SQLException {
        synchronized (sync) {
            if (SysProperties.CHECK && end) {
                throw Message.getInternalError();
            }
            while (true) {
                if (needNewDelta) {
                    loadNext(false);
                    needNewDelta = false;
                }
                if (needNewBase) {
                    loadNext(true);
                    needNewBase = false;
                }
                if (deltaRow == null) {
                    if (baseRow == null) {
                        end = true;
                        return false;
                    } else {
                        onBase = true;
                        needNewBase = true;
                        return true;
                    }
                }
                int sessionId = deltaRow.getSessionId();
                boolean isThisSession = sessionId == session.getId();
                boolean isDeleted = deltaRow.getDeleted();
                if (isThisSession && isDeleted) {
                    needNewDelta = true;
                    continue;
                }
                if (baseRow == null) {
                    if (isDeleted) {
                        if (isThisSession) {
                            end = true;
                            return false;
                        } else {
                            // the row was deleted by another session: return it
                            onBase = false;
                            needNewDelta = true;
                            return true;
                        }
                    }
                    throw Message.getInternalError();
                }
                int compare = index.compareRows(deltaRow, baseRow);
                if (compare == 0) {
                    compare = index.compareKeys(deltaRow, baseRow);
                }
                if (compare == 0) {
                    if (isDeleted) {
                        if (isThisSession) {
                            throw Message.getInternalError();
                        } else {
                            // another session updated the row: must be deleted
                            // in base as well
                            throw Message.getInternalError();
                        }
                    } else {
                        if (isThisSession) {
                            onBase = false;
                            needNewBase = true;
                            needNewDelta = true;
                            return true;
                        } else {
                            // another session inserted the row: ignore
                            needNewBase = true;
                            needNewDelta = true;
                            continue;
                        }
                    }
                }
                if (compare > 0) {
                    needNewBase = true;
                    return true;
                }
                if (!isDeleted) {
                    throw Message.getInternalError();
                }
                needNewDelta = true;
                return true;
            }
        }
    }
}
