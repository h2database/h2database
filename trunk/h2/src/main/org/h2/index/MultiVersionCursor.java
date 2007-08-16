package org.h2.index;

import java.sql.SQLException;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;

public class MultiVersionCursor implements Cursor {
    
    private final MultiVersionIndex index;
    private final Session session;
    private final Cursor base, delta;
    private boolean onBase;
    private SearchRow current;
    
    MultiVersionCursor(Session session, MultiVersionIndex index, Cursor base, Cursor delta) throws SQLException {
        this.session = session;
        this.index = index;
        this.base = base;
        this.delta = delta;
        boolean b = base.next();
        boolean d = delta.next();
    }

    public Row get() throws SQLException {
        return onBase ? base.get() : delta.get();
    }

    public int getPos() {
        return onBase ? base.getPos() : delta.getPos();
    }

    public SearchRow getSearchRow() throws SQLException {
        return onBase ? base.getSearchRow() : delta.getSearchRow();
    }

    public boolean next() throws SQLException {
        return false;
    }
}
